/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flight_sql_server.h"
#include "observer/flight_sql/ob_flightsql_session.h"
#include "observer/flight_sql/ob_flight_worker.h"
#include "observer/ob_srv_network_frame.h"

// OceanBase headers
#include "lib/utility/ob_defer.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "lib/ob_define.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_se_array.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/string/ob_sql_string.h"
#include "lib/worker.h"
#include "lib/oblog/ob_log.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "lib/trace/ob_trace.h"
#include "lib/worker.h"
#include "share/ob_errno.h"
#include "share/ob_server_struct.h"
#include "share/ob_cluster_version.h"

// Arrow Flight headers (must come after OB headers to avoid namespace collisions)
#include "arrow/flight/server.h"
#include "arrow/flight/server_auth.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/sql/types.h"
#include "arrow/flight/types.h"
#include "arrow/array.h"
#include "arrow/array/util.h"
#include "arrow/scalar.h"
#include "arrow/record_batch.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/io/memory.h"
#include "arrow/util/base64.h"
#include "arrow/util/compression.h"
#include "arrow/util/key_value_metadata.h"
#include "observer/flight_sql/ob_flightsql_query_processor.h"
#include "observer/flight_sql/ob_flightsql_arrow_converter.h"
#include "observer/flight_sql/ob_flightsql_param_converter.h"
#include "lib/profile/ob_trace_id.h"
#include "observer/flight_sql/ob_flight_batch_reader.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_result_set.h"

#include "share/config/ob_server_config.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "observer/flight_sql/ob_flight_sql_audit.h"
#include "observer/flight_sql/ob_flightsql_ingest_registry.h"
#include "sql/parser/ob_parser.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/flight_sql/ob_flightsql_rpc_cancel.h"
#include <grpcpp/resource_quota.h>
#include <grpcpp/server_builder.h>
#include <openssl/crypto.h>

#include <atomic>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <variant>
#include <vector>

namespace oceanbase
{
namespace observer
{
namespace sql = ::oceanbase::sql;

namespace
{

// ============================================================
// GetSqlInfo custom codes  (vendor-specific range: ≥ 10000)
// ============================================================

// Vendor key 10000: OceanBase cluster version (e.g. "4.6.0.1"), populated from GET_MIN_CLUSTER_VERSION().
static constexpr int32_t  kObClusterVersion = 10000;
static constexpr int32_t  kObSqlInfoOptFeatureVersion      = 10001;
// Value for standard key 1 (FLIGHT_SQL_SERVER_VERSION): Flight SQL protocol version implemented
// by this server. Bump for new APIs and fixes that clients must gate on.
// 1.2.3 adds V1 whole-table and named-partition INSERT OVERWRITE ingest.
static constexpr const char *kObFlightSqlImplVersion       = "1.2.3";

static void flight_sql_cleanse_string(std::string &value)
{
  if (!value.empty()) {
    OPENSSL_cleanse(&value[0], value.size());
    value.clear();
  }
}

static void flight_sql_cleanse_ob_string(common::ObString &value)
{
  if (!value.empty()) {
    OPENSSL_cleanse(value.ptr(), static_cast<size_t>(value.length()));
    value.reset();
  }
}

static std::string flight_sql_make_prepared_handle(const uint64_t generation)
{
  const uint64_t nonce = ::oceanbase::trace::UUID::gen_rand();
  char handle[64];
  snprintf(handle, sizeof(handle), "ps-%016llx-%016llx",
           static_cast<unsigned long long>(generation),
           static_cast<unsigned long long>(nonce));
  return std::string(handle);
}

// ============================================================
// SetSessionOptions / GetSessionOptions key names
// ============================================================
static constexpr const char *kSessionOptCatalog = "catalog";
static constexpr const char *kSessionOptSchema = "schema";
static constexpr const char *kSessionOptDbSchema = "db_schema";   // legacy alias for schema
static constexpr const char *kSessionOptObSessionVars = "ob_session_vars";
static constexpr const char *kSessionOptBatchSize = "ob_flight_sql_batch_size";
static constexpr const char *kSessionOptCompression = "arrow.ipc.compression";

// Format a packed uint64 OceanBase version into "major.minor.patch.bp" (e.g. "4.6.0.0").
static std::string ob_format_version(uint64_t ver)
{
  char buf[64] = {};
  common::ObClusterVersion::print_version_str(buf, static_cast<int64_t>(sizeof(buf) - 1), ver);
  return std::string(buf);
}

// ============================================================
// Session vars helpers
// ============================================================

static std::string url_decode(const std::string &s)
{
  std::string out;
  out.reserve(s.size());
  for (size_t i = 0; i < s.size(); ) {
    if (s[i] == '%' && i + 2 < s.size() && isxdigit(s[i+1]) && isxdigit(s[i+2])) {
      char buf[3] = { s[i+1], s[i+2], '\0' };
      out += static_cast<char>(strtol(buf, nullptr, 16));
      i += 3;
    } else if (s[i] == '+') {
      out += ' ';
      i++;
    } else {
      out += s[i++];
    }
  }
  return out;
}

static bool is_valid_var_name(const std::string &name)
{
  if (name.empty()) { return false; }
  for (char c : name) {
    if (!isalnum(c) && c != '_') { return false; }
  }
  return true;
}

// MySQL-mode identifiers are inserted into generated SQL inside backticks.
// Reject the quote character and controls instead of accepting text that can
// terminate an identifier or make audit/log output ambiguous.  Oracle mode
// applies the stricter regular-identifier check below.
static bool flight_sql_is_safe_identifier(const std::string &name)
{
  if (name.empty()) { return false; }
  for (const unsigned char c : name) {
    if ('`' == c || '\0' == c || c < 0x20 || 0x7f == c) { return false; }
  }
  return true;
}

// Oracle folds regular, unquoted identifiers to upper case.  Flight clients
// commonly pass the lower-case spelling used in unquoted CREATE TABLE SQL, so
// double-quoting that spelling would address a different, case-sensitive
// object.  Keep Oracle identifiers unquoted and restrict them to the regular
// identifier grammar before inserting them into generated SQL.
static bool flight_sql_is_safe_oracle_identifier(const std::string &name)
{
  if (name.empty()
      || !(('a' <= name[0] && name[0] <= 'z')
           || ('A' <= name[0] && name[0] <= 'Z'))) {
    return false;
  }
  for (size_t i = 1; i < name.size(); ++i) {
    const char c = name[i];
    if (!(('a' <= c && c <= 'z')
          || ('A' <= c && c <= 'Z')
          || ('0' <= c && c <= '9')
          || '_' == c || '$' == c || '#' == c)) {
      return false;
    }
  }
  return true;
}

static void flight_sql_append_identifier(std::string &sql,
                                         const std::string &name,
                                         const bool is_oracle_mode)
{
  if (is_oracle_mode) {
    sql.append(name);
  } else {
    sql.push_back('`');
    sql.append(name);
    sql.push_back('`');
  }
}

static bool flight_sql_is_safe_hint(const std::string &hint)
{
  return hint.find("*/") == std::string::npos
      && hint.find(';') == std::string::npos
      && hint.find("--") == std::string::npos;
}

// Apply URL-encoded "k=v,k=v" via update_sys_variable_by_set_session (SET SESSION path).
// All pairs are attempted regardless of individual failures.
// Failed keys are collected into `errors` (key → Arrow SetSessionOptionErrorValue)
// so the caller can report them per-key to the client without aborting the call.
using SessionVarErrors = std::map<std::string, arrow::flight::SetSessionOptionsResult::Error>;

// One mirrored SQL audit row per option (`mirror_*` builds QUERY_SQL as "SET SESSION " + kv).
// flightsql_audit_format_kv_ builds the "name=value" tail only.
static std::string flightsql_audit_format_kv_(const std::string &name, const std::string &value)
{
  std::string line;
  line.reserve(name.size() + 1 + value.size());
  line.append(name);
  line.push_back('=');
  line.append(value);
  return line;
}

static void mirror_flightsql_session_kv_to_sql_audit(ObFlightSqlSession &session,
                                                      const std::string &kv_line,
                                                      const int ob_ret_code,
                                                      const char *request_name)
{
  try {
  const int64_t now = common::ObTimeUtility::current_time();
  ObFlightSqlAuditRecord rec;
  rec.request_id = now;
  rec.tenant_id = session.tenant_id;
  rec.plan_id = 0;
  rec.request_time = now;
  rec.elapsed_time = 0;
  rec.execute_time = 0;
  rec.return_rows = 0;
  rec.ret_code = ob_ret_code;
  rec.peer_port = session.peer_port;
#define SAFE_COPY(dst, src) \
  do { strncpy(dst, (src).c_str(), sizeof(dst) - 1); dst[sizeof(dst) - 1] = '\0'; } while (0)
  SAFE_COPY(rec.user_name, session.user_name);
  SAFE_COPY(rec.schema_name, session.current_schema);
  SAFE_COPY(rec.catalog_name, session.current_catalog);
  SAFE_COPY(rec.peer_ip, session.peer_ip);
#undef SAFE_COPY
  rec.ob_session_id = OB_NOT_NULL(session.ob_session) ? session.ob_session->get_server_sid() : 0;
  rec.sql_id[0] = '\0';
  {
    const char *tid = common::ObCurTraceId::get_trace_id_str();
    if (OB_NOT_NULL(tid)) {
      strncpy(rec.trace_id, tid, sizeof(rec.trace_id) - 1);
      rec.trace_id[sizeof(rec.trace_id) - 1] = '\0';
    } else {
      rec.trace_id[0] = '\0';
    }
  }
  {
    static const char SET_SESSION_PREFIX[] = "SET SESSION ";
    std::string sql;
    sql.reserve(sizeof(SET_SESSION_PREFIX) - 1 + kv_line.size());
    sql.append(SET_SESSION_PREFIX);
    sql.append(kv_line);
    rec.sql_text = std::move(sql);
  }

  if (OB_NOT_NULL(session.ob_session)) {
    mirror_flight_sql_audit_record_to_gv_sql_audit(
        *session.ob_session, rec, request_name);
  }
  } catch (const std::bad_alloc &) {
    // skip audit on OOM — LOG_WARN may itself allocate
  } catch (const std::exception &e) {
    int ret = OB_SUCCESS;
    LOG_WARN("[FlightSQL] session kv audit: unexpected exception, skipped", "what", e.what());
  } catch (...) {}
}

static void apply_session_vars(ObFlightSqlSession &session,
                                const std::string &encoded,
                                SessionVarErrors &errors,
                                const char *request_name)
{
  int ret = OB_SUCCESS;
  size_t start = 0;
  if (OB_ISNULL(session.ob_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] apply_session_vars: ob_session is null", K(ret));
  } else {
    while (start <= encoded.size()) {
      const size_t comma = encoded.find(',', start);
      const std::string pair = encoded.substr(start, comma == std::string::npos
                                                         ? std::string::npos
                                                         : comma - start);
      start = (comma == std::string::npos) ? encoded.size() + 1 : comma + 1;
      if (pair.empty()) { continue; }

      const size_t eq = pair.find('=');
      if (eq == std::string::npos) {
        LOG_WARN("[FlightSQL] apply_session_vars: malformed pair, skipping",
                 "pair", pair.c_str());
        continue;
      }
      const std::string var_name  = url_decode(pair.substr(0, eq));
      const std::string var_value = url_decode(pair.substr(eq + 1));

      if (!is_valid_var_name(var_name)) {
        LOG_WARN("[FlightSQL] apply_session_vars: invalid var name", "name", var_name.c_str());
        errors[var_name] = {arrow::flight::SetSessionOptionErrorValue::kInvalidName};
        continue;
      }

      const common::ObString ob_name(var_name.size(), var_name.c_str());
      const common::ObString ob_value(var_value.size(), var_value.c_str());
      const int rc = session.ob_session->update_sys_variable_by_set_session(ob_name, ob_value);
      if (OB_SUCCESS != rc) {
        LOG_WARN("[FlightSQL] apply_session_vars: update_sys_variable_by_set_session failed",
                 K(rc), "var", var_name.c_str(), "value", var_value.c_str());
        arrow::flight::SetSessionOptionErrorValue ev;
        if (rc == OB_ERR_SYS_VARIABLE_UNKNOWN) {
          ev = arrow::flight::SetSessionOptionErrorValue::kInvalidName;
        } else if (rc == OB_ERR_WRONG_TYPE_FOR_VAR || rc == OB_ERR_WRONG_VALUE_FOR_VAR) {
          ev = arrow::flight::SetSessionOptionErrorValue::kInvalidValue;
        } else {
          ev = arrow::flight::SetSessionOptionErrorValue::kError;
        }
        errors[var_name] = {ev};
        mirror_flightsql_session_kv_to_sql_audit(session,
                                                     flightsql_audit_format_kv_(var_name, var_value),
                                                 rc, request_name);
      } else {
        LOG_TRACE("[FlightSQL] apply_session_vars: update_sys_variable_by_set_session OK",
                 "var", var_name.c_str());
        mirror_flightsql_session_kv_to_sql_audit(session,
                                                     flightsql_audit_format_kv_(var_name, var_value),
                                                 OB_SUCCESS, request_name);
      }
    }
  }
  return;
}

using ArrowCodecResult = arrow::Result<std::unique_ptr<arrow::util::Codec>>;

// gRPC header key constants
static constexpr const char *FLIGHT_SQL_HEADER_AUTHORIZATION = "authorization";
static constexpr const char *FLIGHT_SQL_HEADER_WRITE_PARALLEL = "ob_write_parallel";
static constexpr const char *FLIGHT_SQL_HEADER_WRITE_MODE        = "ob_write_mode";
static constexpr const char *FLIGHT_SQL_HEADER_DIRECT_LOAD       = "ob_direct_load";
static constexpr const char *FLIGHT_SQL_HEADER_WRITE_HINT        = "ob_write_hint";
static constexpr const char *FLIGHT_SQL_HEADER_WRITE_KEY_COLUMNS = "ob_write_key_columns";
static constexpr const char *FLIGHT_SQL_HEADER_MERGE_OPTIONS     = "ob_merge_options";
static constexpr const char *FLIGHT_SQL_HEADER_OVERWRITE_OPTIONS = "ob_overwrite_options";
static constexpr const char *FLIGHT_SQL_WRITE_MODE_INSERT        = "insert";
static constexpr const char *FLIGHT_SQL_WRITE_MODE_UPSERT        = "upsert";
static constexpr const char *FLIGHT_SQL_WRITE_MODE_UPDATE        = "update";
static constexpr const char *FLIGHT_SQL_WRITE_MODE_MERGE         = "merge";
static constexpr const char *FLIGHT_SQL_WRITE_MODE_OVERWRITE     = "overwrite";

static int flight_sql_set_write_parallel_(const std::string &value,
                                          int &write_parallel)
{
  int ret = OB_SUCCESS;
  try {
    std::string::size_type parsed_size = 0;
    const int parsed_parallel = std::stoi(value, &parsed_size);
    if (parsed_size != value.size() || parsed_parallel <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      write_parallel = parsed_parallel;
    }
  } catch (...) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

static int flight_sql_set_write_mode_(const common::ObString &value,
                                      bool &is_upsert,
                                      bool &is_update,
                                      bool &is_merge,
                                      bool &is_overwrite)
{
  int ret = OB_SUCCESS;
  is_upsert = false;
  is_update = false;
  is_merge = false;
  is_overwrite = false;
  if (value.empty() || value == FLIGHT_SQL_WRITE_MODE_INSERT) {
    // INSERT is the default append behavior.
  } else if (value == FLIGHT_SQL_WRITE_MODE_UPSERT) {
    is_upsert = true;
  } else if (value == FLIGHT_SQL_WRITE_MODE_UPDATE) {
    is_update = true;
  } else if (value == FLIGHT_SQL_WRITE_MODE_MERGE) {
    is_merge = true;
  } else if (value == FLIGHT_SQL_WRITE_MODE_OVERWRITE) {
    is_overwrite = true;
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

static arrow::Status flight_sql_parse_overwrite_options_(
    const common::ObString &json, common::ObIAllocator &allocator,
    const bool is_oracle_mode, std::vector<std::string> &partitions)
{
  int ret = OB_SUCCESS;
  common::ObJsonNode *root = nullptr;
  common::ObIJsonBase *names = nullptr;
  partitions.clear();
  if (json.empty() || json.length() > 64 * 1024 || OB_NOT_NULL(json.find('\0'))) {
    return arrow::Status::Invalid(
        "[FlightSQL] ob_overwrite_options must contain JSON of at most 64 KiB");
  } else if (OB_FAIL(common::ObJsonParser::get_tree(&allocator, json, root,
      common::ObJsonParser::JSN_STRICT_FLAG | common::ObJsonParser::JSN_UNIQUE_FLAG))) {
    return arrow::Status::Invalid(
        "[FlightSQL] ob_overwrite_options must be valid JSON with unique keys");
  } else if (OB_ISNULL(root) || root->json_type() != common::ObJsonNodeType::J_OBJECT
             || root->element_count() != 1) {
    return arrow::Status::Invalid(
        "[FlightSQL] ob_overwrite_options must be an object containing only partitions");
  } else if (OB_FAIL(root->get_object_value(common::ObString::make_string("partitions"), names))
             || OB_ISNULL(names) || names->json_type() != common::ObJsonNodeType::J_ARRAY
             || names->element_count() == 0) {
    return arrow::Status::Invalid(
        "[FlightSQL] overwrite partitions must be a nonempty array of partition names");
  }
  for (uint64_t i = 0; i < names->element_count(); ++i) {
    common::ObIJsonBase *node = nullptr;
    if (OB_FAIL(names->get_array_element(i, node)) || OB_ISNULL(node)
        || node->json_type() != common::ObJsonNodeType::J_STRING) {
      return arrow::Status::Invalid("[FlightSQL] overwrite partition names must be strings");
    }
    const common::ObString name(
        static_cast<int32_t>(node->get_data_length()), node->get_data());
    const std::string partition(name.ptr(), name.length());
    if (name.trim().empty() || !flight_sql_is_safe_identifier(partition)
        || partition.find('"') != std::string::npos
        || (is_oracle_mode && !flight_sql_is_safe_oracle_identifier(partition))) {
      return arrow::Status::Invalid(
          "[FlightSQL] overwrite partition names must be nonempty identifiers without SQL quotes or control characters");
    }
    for (const std::string &existing : partitions) {
      const common::ObString previous(existing.size(), existing.data());
      if (common::ObCharset::case_insensitive_equal(name, previous)) {
        return arrow::Status::Invalid("[FlightSQL] duplicate overwrite partition name");
      }
    }
    partitions.push_back(partition);
  }
  return arrow::Status::OK();
}

static int flight_sql_set_direct_load_(const std::string &value,
                                       bool &direct_load)
{
  int ret = OB_SUCCESS;
  if (value.empty() || value == "false" || value == "0") {
    direct_load = false;
  } else if (value == "true" || value == "1") {
    direct_load = true;
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

static int flight_sql_set_write_hint_(const std::string &value,
                                      std::string &extra_hint)
{
  int ret = OB_SUCCESS;
  extra_hint.clear();
  if (!value.empty()) {
    if (!flight_sql_is_safe_hint(value)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      extra_hint = value;
    }
  }
  return ret;
}

static int flight_sql_set_write_key_columns_(const std::string &value,
                                             std::vector<std::string> &key_cols)
{
  int ret = OB_SUCCESS;
  key_cols.clear();
  std::stringstream ss(value);
  std::string col;
  while (OB_SUCC(ret) && std::getline(ss, col, ',')) {
    const std::string::size_type s = col.find_first_not_of(" \t");
    const std::string::size_type e = col.find_last_not_of(" \t");
    if (s != std::string::npos) {
      const std::string key_col = col.substr(s, e - s + 1);
      if (!flight_sql_is_safe_identifier(key_col)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        key_cols.push_back(key_col);
      }
    }
  }
  return ret;
}

static arrow::Status flight_sql_validate_update_key_columns_(
    const std::vector<std::string> &schema_cols,
    const std::vector<std::string> &key_cols,
    std::unordered_set<std::string> &key_set)
{
  key_set.clear();
  const std::unordered_set<std::string> schema_col_set(
      schema_cols.begin(), schema_cols.end());
  for (const std::string &key_col : key_cols) {
    if (schema_col_set.find(key_col) == schema_col_set.end()) {
      return arrow::Status::Invalid(
          "[FlightSQL] key column '" + key_col + "' not found in Arrow schema");
    } else if (!key_set.insert(key_col).second) {
      return arrow::Status::Invalid(
          "[FlightSQL] duplicate key column '" + key_col + "'");
    }
  }

  bool has_non_key_col = false;
  for (const std::string &schema_col : schema_cols) {
    if (key_set.find(schema_col) == key_set.end()) {
      has_non_key_col = true;
      break;
    }
  }
  if (!has_non_key_col) {
    return arrow::Status::Invalid(
        "[FlightSQL] update mode: no non-key columns to update "
        "(all columns are listed as key columns).");
  }
  return arrow::Status::OK();
}

// V1 MERGE options borrow strings from a JSON tree owned by the DoPut arena.
// Each branch maps to one native clause; DELETE WHERE belongs to UPDATE.
class ObFlightSqlMergeOptions
{
public:
  ObFlightSqlMergeOptions(common::ObIAllocator &allocator, uint64_t tenant_id)
      : allocator_(allocator), tenant_id_(tenant_id),
        target_alias_(common::ObString::make_string("t")),
        source_alias_(common::ObString::make_string("s")),
        update_assignments_(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                            common::ModulePageAllocator(allocator, "FlightSQLMerge")),
        insert_values_(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                       common::ModulePageAllocator(allocator, "FlightSQLMerge"))
  {}

  int parse(const common::ObString &json, common::ObString &error);
  int validate_expressions(ObSQLMode sql_mode, const sql::ObCharsets4Parser &charsets,
                           common::ObString &error) const;
  int build_sql(const common::ObString &db, const common::ObString &table,
                uint64_t stream_id, bool has_parallel, int parallel,
                const common::ObString &extra_hint, bool oracle_mode,
                common::ObSqlString &sql) const;

private:
  struct Assignment
  {
    common::ObString column_;
    common::ObString value_;
    TO_STRING_KV(K_(column), K_(value));
  };
  typedef common::ObSEArray<Assignment, 8> Assignments;

  static int invalid_(const char *message, common::ObString &error)
  {
    error = common::ObString::make_string(message);
    return OB_INVALID_ARGUMENT;
  }
  static bool is_identifier_(const common::ObString &name)
  {
    bool valid = !name.empty();
    for (int64_t i = 0; valid && i < name.length(); ++i) {
      const unsigned char c = name[i];
      valid = c >= 0x20 && c != 0x7f && c != '`' && c != '"';
    }
    return valid;
  }
  static bool is_alias_(const common::ObString &name)
  {
    bool valid = !name.empty();
    for (int64_t i = 0; valid && i < name.length(); ++i) {
      const unsigned char c = name[i];
      valid = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
          || c == '_' || (i > 0 && c >= '0' && c <= '9');
    }
    return valid;
  }
  static int validate_object_(const common::ObIJsonBase &object,
                              const char *const *allowed, int64_t allowed_count,
                              common::ObString &error);
  static int read_string_(const common::ObIJsonBase &object, const char *key,
                          bool required, common::ObString &value, common::ObString &error);
  static int read_assignments_(const common::ObIJsonBase &object, const char *key,
                               Assignments &values, common::ObString &error);
  int validate_expression_(const common::ObString &expression, bool predicate,
                            ObSQLMode sql_mode, const sql::ObCharsets4Parser &charsets,
                            common::ObString &error) const;
  static int append_predicate_(const char *clause, const common::ObString &predicate,
                               common::ObSqlString &sql);
  static int append_value_(const common::ObString &value, common::ObSqlString &sql);

  common::ObIAllocator &allocator_;
  uint64_t tenant_id_;
  common::ObString condition_;
  common::ObString target_alias_;
  common::ObString source_alias_;
  Assignments update_assignments_;
  common::ObString update_condition_;
  common::ObString delete_condition_;
  Assignments insert_values_;
  common::ObString insert_condition_;
  DISALLOW_COPY_AND_ASSIGN(ObFlightSqlMergeOptions);
};

int ObFlightSqlMergeOptions::validate_object_(const common::ObIJsonBase &object,
    const char *const *allowed, int64_t allowed_count, common::ObString &error)
{
  int ret = OB_SUCCESS;
  if (object.json_type() != common::ObJsonNodeType::J_OBJECT) {
    ret = invalid_("merge options and clauses must be JSON objects", error);
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < object.element_count(); ++i) {
      common::ObString key;
      bool found = false;
      if (OB_FAIL(object.get_key(i, key))) {
        LOG_WARN("[FlightSQL] failed to read merge option name", K(ret));
      } else {
        for (int64_t j = 0; !found && j < allowed_count; ++j) {
          found = key == allowed[j];
        }
        if (!found) {
          ret = invalid_("unknown merge option or clause member", error);
          LOG_WARN("[FlightSQL] unknown merge option", K(ret), K(key));
        }
      }
    }
  }
  return ret;
}

int ObFlightSqlMergeOptions::read_string_(const common::ObIJsonBase &object,
    const char *key, bool required, common::ObString &value, common::ObString &error)
{
  int ret = OB_SUCCESS;
  common::ObIJsonBase *node = nullptr;
  const int lookup_ret = object.get_object_value(common::ObString::make_string(key), node);
  if (lookup_ret == OB_SEARCH_NOT_FOUND && !required) {
    // Keep defaults for omitted optional strings.
  } else if (lookup_ret != OB_SUCCESS || OB_ISNULL(node)
             || node->json_type() != common::ObJsonNodeType::J_STRING) {
    ret = invalid_("merge expressions and aliases must be strings", error);
  } else {
    value = common::ObString(static_cast<int32_t>(node->get_data_length()), node->get_data());
    if (value.trim().empty() || OB_NOT_NULL(value.find('\0'))) {
      ret = invalid_("merge strings must not be empty or contain NUL", error);
    }
  }
  return ret;
}

int ObFlightSqlMergeOptions::read_assignments_(const common::ObIJsonBase &object,
    const char *key, Assignments &values, common::ObString &error)
{
  int ret = OB_SUCCESS;
  common::ObIJsonBase *node = nullptr;
  if (OB_FAIL(object.get_object_value(common::ObString::make_string(key), node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      ret = invalid_("merge assignments or values are required", error);
    } else {
      LOG_WARN("[FlightSQL] failed to read merge assignments", K(ret));
    }
  } else if (OB_ISNULL(node) || node->json_type() != common::ObJsonNodeType::J_OBJECT
             || node->element_count() == 0) {
    ret = invalid_("merge assignments and values must be nonempty JSON objects", error);
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < node->element_count(); ++i) {
      Assignment assignment;
      common::ObIJsonBase *value = nullptr;
      bool duplicate = false;
      if (OB_FAIL(node->get_object_value(i, assignment.column_, value))) {
        LOG_WARN("[FlightSQL] failed to read merge assignment", K(ret));
      } else {
        for (int64_t j = 0; !duplicate && j < values.count(); ++j) {
          duplicate = 0 == assignment.column_.case_compare(values.at(j).column_);
        }
        if (!is_identifier_(assignment.column_) || duplicate) {
          ret = invalid_("invalid or duplicate merge target column", error);
        } else if (OB_ISNULL(value) || value->json_type() != common::ObJsonNodeType::J_STRING) {
          ret = invalid_("merge assignment values must be SQL expression strings", error);
        } else {
          assignment.value_ = common::ObString(
              static_cast<int32_t>(value->get_data_length()), value->get_data());
          if (assignment.value_.trim().empty() || OB_NOT_NULL(assignment.value_.find('\0'))) {
            ret = invalid_("merge assignment expressions must not be empty or contain NUL", error);
          } else if (OB_FAIL(values.push_back(assignment))) {
            LOG_WARN("[FlightSQL] failed to store merge assignment", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObFlightSqlMergeOptions::parse(const common::ObString &json, common::ObString &error)
{
  int ret = OB_SUCCESS;
  error = common::ObString::make_string("invalid merge options");
  common::ObJsonNode *root = nullptr;
  const char *const root_keys[] = {
    "condition", "target_alias", "source_alias", "matched_update", "not_matched_insert"};
  const char *const update_keys[] = {"assignments", "condition", "delete_condition"};
  const char *const insert_keys[] = {"values", "condition"};
  if (json.empty() || json.length() > 64 * 1024 || OB_NOT_NULL(json.find('\0'))) {
    ret = invalid_("ob_merge_options must contain JSON of at most 64 KiB", error);
  } else if (OB_FAIL(common::ObJsonParser::get_tree(&allocator_, json, root,
      common::ObJsonParser::JSN_STRICT_FLAG | common::ObJsonParser::JSN_UNIQUE_FLAG))) {
    error = common::ObString::make_string("ob_merge_options must be valid JSON with unique keys");
  } else if (OB_ISNULL(root)) {
    ret = invalid_("ob_merge_options must be a JSON object", error);
  } else if (OB_FAIL(validate_object_(*root, root_keys, ARRAYSIZEOF(root_keys), error))) {
  } else if (OB_FAIL(read_string_(*root, "condition", true, condition_, error))) {
  } else if (OB_FAIL(read_string_(*root, "target_alias", false, target_alias_, error))) {
  } else if (OB_FAIL(read_string_(*root, "source_alias", false, source_alias_, error))) {
  } else if (!is_alias_(target_alias_) || !is_alias_(source_alias_)
             || 0 == target_alias_.case_compare(source_alias_)) {
    ret = invalid_("merge aliases must be distinct simple SQL identifiers", error);
  } else {
    common::ObIJsonBase *update = nullptr;
    common::ObIJsonBase *insert = nullptr;
    const int update_ret = root->get_object_value(common::ObString::make_string("matched_update"), update);
    const int insert_ret = root->get_object_value(common::ObString::make_string("not_matched_insert"), insert);
    if (update_ret == OB_SUCCESS && OB_NOT_NULL(update)) {
      if (OB_FAIL(validate_object_(*update, update_keys, ARRAYSIZEOF(update_keys), error))) {
      } else if (OB_FAIL(read_assignments_(*update, "assignments", update_assignments_, error))) {
      } else if (OB_FAIL(read_string_(*update, "condition", false, update_condition_, error))) {
      } else if (OB_FAIL(read_string_(*update, "delete_condition", false, delete_condition_, error))) {
      }
    } else if (update_ret != OB_SEARCH_NOT_FOUND) {
      ret = invalid_("invalid matched_update clause", error);
    }
    if (OB_SUCC(ret)) {
      if (insert_ret == OB_SUCCESS && OB_NOT_NULL(insert)) {
        if (OB_FAIL(validate_object_(*insert, insert_keys, ARRAYSIZEOF(insert_keys), error))) {
        } else if (OB_FAIL(read_assignments_(*insert, "values", insert_values_, error))) {
        } else if (OB_FAIL(read_string_(*insert, "condition", false, insert_condition_, error))) {
        }
      } else if (insert_ret != OB_SEARCH_NOT_FOUND) {
        ret = invalid_("invalid not_matched_insert clause", error);
      }
    }
    if (OB_SUCC(ret) && update_assignments_.empty() && insert_values_.empty()) {
      ret = invalid_("merge requires matched_update or not_matched_insert", error);
    }
  }
  return ret;
}

int ObFlightSqlMergeOptions::validate_expression_(const common::ObString &expression,
    bool predicate, ObSQLMode sql_mode, const sql::ObCharsets4Parser &charsets,
    common::ObString &error) const
{
  int ret = OB_SUCCESS;
  if (!predicate && 0 == expression.trim().case_compare("DEFAULT")) {
    // DEFAULT is a native assignment value, rather than a SELECT expression.
  } else {
    common::ObSqlString query;
    query.set_attr(common::ObMemAttr(tenant_id_, "FlightSQLMerge"));
    ParseResult result{};
    sql::ObParser parser(allocator_, sql_mode, charsets);
    if (OB_FAIL(query.assign_fmt(predicate ? "SELECT 1 FROM DUAL WHERE (\n%.*s\n)"
                                           : "SELECT (\n%.*s\n) FROM DUAL",
                                 expression.length(), expression.ptr()))) {
      LOG_WARN("[FlightSQL] failed to build merge expression query", K(ret));
    } else if (OB_FAIL(parser.parse(query.string(), result, STD_MODE, false, true))) {
      error = common::ObString::make_string("invalid merge SQL expression");
    } else {
      const ParseNode *root = result.result_tree_;
      const ParseNode *select = OB_NOT_NULL(root) && root->type_ == T_STMT_LIST
          && root->num_child_ == 1 && OB_NOT_NULL(root->children_)
          ? root->children_[0] : nullptr;
      const ParseNode *projects = OB_NOT_NULL(select) && select->type_ == T_SELECT
          && select->num_child_ == PARSE_SELECT_MAX_IDX && OB_NOT_NULL(select->children_)
          ? select->children_[PARSE_SELECT_SELECT] : nullptr;
      const ParseNode *project = OB_NOT_NULL(projects) && projects->num_child_ == 1
          && OB_NOT_NULL(projects->children_) ? projects->children_[0] : nullptr;
      if (OB_ISNULL(project) || project->type_ != T_PROJECT_STRING
          || project->num_child_ != 1 || OB_ISNULL(project->children_)
          || OB_ISNULL(project->children_[0]) || project->children_[0]->type_ == T_ALIAS
          || result.question_mark_ctx_.count_ != 0) {
        ret = invalid_("merge options require individual SQL expressions without parameters", error);
      } else {
        // Both native parsers omit the generated, unaliased FROM DUAL node.
        for (int64_t i = 0; OB_SUCC(ret) && i < select->num_child_; ++i) {
          if (i != PARSE_SELECT_SELECT && !(predicate && i == PARSE_SELECT_WHERE)
              && OB_NOT_NULL(select->children_[i])) {
            ret = invalid_("merge expressions must not add SQL clauses or statements", error);
          }
        }
      }
    }
    parser.free_result(result);
  }
  return ret;
}

int ObFlightSqlMergeOptions::validate_expressions(ObSQLMode sql_mode,
    const sql::ObCharsets4Parser &charsets, common::ObString &error) const
{
  int ret = OB_SUCCESS;
  const common::ObString *predicates[] = {
    &condition_, &update_condition_, &delete_condition_, &insert_condition_};
  const Assignments *assignments[] = {&update_assignments_, &insert_values_};
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(predicates); ++i) {
    if (!predicates[i]->empty()
        && OB_FAIL(validate_expression_(*predicates[i], true, sql_mode, charsets, error))) {
      LOG_WARN("[FlightSQL] invalid merge predicate", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(assignments); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < assignments[i]->count(); ++j) {
      if (OB_FAIL(validate_expression_(assignments[i]->at(j).value_, false,
                                       sql_mode, charsets, error))) {
        LOG_WARN("[FlightSQL] invalid merge assignment", K(ret));
      }
    }
  }
  return ret;
}

int ObFlightSqlMergeOptions::append_predicate_(const char *clause,
    const common::ObString &predicate, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (!predicate.empty()
      && OB_FAIL(sql.append_fmt("%s(\n%.*s\n)", clause, predicate.length(), predicate.ptr()))) {
    LOG_WARN("[FlightSQL] failed to append merge predicate", K(ret));
  }
  return ret;
}

int ObFlightSqlMergeOptions::append_value_(const common::ObString &value,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (0 == value.trim().case_compare("DEFAULT")) {
    ret = sql.append("DEFAULT");
  } else {
    ret = sql.append_fmt("(\n%.*s\n)", value.length(), value.ptr());
  }
  return ret;
}

int ObFlightSqlMergeOptions::build_sql(const common::ObString &db,
    const common::ObString &table, uint64_t stream_id, bool has_parallel, int parallel,
    const common::ObString &extra_hint, bool oracle_mode, common::ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  const char quote = oracle_mode ? '"' : '`';
  if (OB_FAIL(sql.assign("MERGE /*+"))) {
  } else if ((!has_parallel || parallel != 1) && OB_FAIL(sql.append(" ENABLE_PARALLEL_DML"))) {
  } else if (!oracle_mode && OB_FAIL(sql.append(" OPT_PARAM('ENABLE_MERGE_INTO', 'true')"))) {
  } else if (has_parallel && OB_FAIL(sql.append_fmt(" PARALLEL(%d)", parallel))) {
  } else if (!extra_hint.empty() && OB_FAIL(sql.append_fmt(" %.*s", extra_hint.length(), extra_hint.ptr()))) {
  } else if (OB_FAIL(sql.append_fmt(
      " */ INTO %c%.*s%c.%c%.*s%c %.*s"
      " USING (SELECT * FROM TABLE(arrow_stream(%lu))) %.*s",
      quote, db.length(), db.ptr(), quote, quote, table.length(), table.ptr(), quote,
      target_alias_.length(), target_alias_.ptr(), stream_id,
      source_alias_.length(), source_alias_.ptr()))) {
  } else if (OB_FAIL(append_predicate_(" ON ", condition_, sql))) {
  }
  if (OB_SUCC(ret) && !update_assignments_.empty()) {
    if (OB_FAIL(sql.append(" WHEN MATCHED THEN UPDATE SET "))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < update_assignments_.count(); ++i) {
        const Assignment &assignment = update_assignments_.at(i);
        if (OB_FAIL(sql.append_fmt("%s%.*s.%c%.*s%c = ", i > 0 ? ", " : "",
            target_alias_.length(), target_alias_.ptr(), quote,
            assignment.column_.length(), assignment.column_.ptr(), quote))) {
        } else if (OB_FAIL(append_value_(assignment.value_, sql))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append_predicate_(" WHERE ", update_condition_, sql))) {
      } else if (OB_FAIL(append_predicate_(" DELETE WHERE ", delete_condition_, sql))) {
      }
    }
  }
  if (OB_SUCC(ret) && !insert_values_.empty()) {
    if (OB_FAIL(sql.append(" WHEN NOT MATCHED THEN INSERT ("))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < insert_values_.count(); ++i) {
        const common::ObString &column = insert_values_.at(i).column_;
        if (OB_FAIL(sql.append_fmt("%s%c%.*s%c", i > 0 ? ", " : "",
                                   quote, column.length(), column.ptr(), quote))) {
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(sql.append(") VALUES ("))) {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < insert_values_.count(); ++i) {
        if (i > 0 && OB_FAIL(sql.append(", "))) {
        } else if (OB_FAIL(append_value_(insert_values_.at(i).value_, sql))) {
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append(")"))) {
        } else if (OB_FAIL(append_predicate_(" WHERE ", insert_condition_, sql))) {
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[FlightSQL] failed to build merge SQL", K(ret));
  }
  return ret;
}

// Multi-stream ingest gRPC headers (set by client on DoPut):
//   ob_ingest_id               — ingest UUID from BeginIngest
//   ob_ingest_streams_per_node — streams per observer (default 1)
static constexpr const char *FLIGHT_SQL_HEADER_INGEST_ID       = "ob_ingest_id";
static constexpr const char *FLIGHT_SQL_HEADER_INGEST_STREAMS_PER_NODE =
    "ob_ingest_streams_per_node";
// The IPC-encoded schema is carried both by ExecuteIngest (for the V3
// coordinator's resolver) and each DoPut (for remote VecOp verification).
static constexpr const char *FLIGHT_SQL_HEADER_INGEST_ARROW_SCHEMA =
    "ob_ingest_arrow_schema";
// V2 ingest: JSON body is detected in GetFlightInfoStatement via "ob_action" key.
// Marker stored in pending_sqls so DoGetStatement can return a typed Arrow result.
static constexpr const char *OB_INGEST_ACTION_MARKER = "__OB_INGEST_ACTION__:";

static constexpr const char *FLIGHT_SQL_HEADER_CATALOG       = "x-ob-catalog";
static constexpr const char *FLIGHT_SQL_HEADER_SCHEMA        = "x-ob-schema";
static constexpr const char *FLIGHT_SQL_HEADER_SESSION_ID    = "x-ob-session-id";

// ============================================================
// OB error code → Arrow Flight status
// ============================================================

static arrow::Status ob_ret_to_flight_status(int ret,
                                             const std::string &tenant_name,
                                             const char *trace_id = nullptr)
{
  const std::string tid = (trace_id && *trace_id) ? std::string(" trace_id=") + trace_id : "";
  switch (ret) {
    case OB_SERVER_IS_INIT:
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unavailable,
          "server is initializing, please retry");
    case OB_ERR_TOO_MANY_SESSIONS:
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unavailable,
          "connection limit reached");
    case OB_PASSWORD_WRONG:
    case OB_ERR_USER_NOT_EXIST:
    case OB_ERR_USER_IS_LOCKED:
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          "invalid username or password");
    case OB_TIMEOUT:
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::TimedOut,
          "session creation timed out for tenant: " + tenant_name +
          " (OB_TIMEOUT); grep WDIAG for" + tid);
    default:
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Internal,
          "failed to create session for tenant: " + tenant_name +
          " (OB-" + std::to_string(-ret) + ")" + tid);
  }
}

// prepare() / execute() failures were returned as arrow::Status::Invalid for every
// OB ret code; Arrow maps Invalid → gRPC INVALID_ARGUMENT, so OB_TIMEOUT (-4012)
// looked like a client "invalid argument" despite the message saying Timeout.
static std::string ob_sql_exec_err_msg_(const int ret,
                                        const char *trace_id,
                                        const std::string &sql_preview)
{
  std::string msg;
  msg.append(ob_strerror(ret));
  msg.append(" (");
  msg.append(ob_error_name(ret));
  msg.append(")");
  if (OB_NOT_NULL(trace_id) && '\0' != trace_id[0]) {
    msg.append(" trace_id=");
    msg.append(trace_id);
  }
  msg.append(" near: ");
  msg.append(sql_preview);
  return msg;
}

static arrow::Status ob_sql_exec_ret_to_arrow_status(const int ret,
                                                    const char *trace_id,
                                                    const std::string &sql_preview,
                                                    const std::string *detail_msg = nullptr)
{
  const std::string msg = (OB_NOT_NULL(detail_msg) && !detail_msg->empty())
      ? *detail_msg : ob_sql_exec_err_msg_(ret, trace_id, sql_preview);
  switch (ret) {
    case OB_TIMEOUT:
    case OB_TRANS_STMT_TIMEOUT:
    case OB_TRANS_TIMEOUT:
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::TimedOut, msg);
    case OB_CANCELED:
    case OB_TRANS_KILLED:
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Cancelled, msg);
    case OB_NOT_MASTER:
    case OB_SWITCHING_TO_FOLLOWER_GRACEFULLY:
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unavailable, msg);
    case OB_EAGAIN:
      return arrow::Status::Invalid("concurrent queries on the same session are not supported");
    case OB_NOT_SUPPORTED:
      return arrow::Status::NotImplemented(std::move(msg));
    default:
      return arrow::Status::Invalid(std::move(msg));
  }
}

// RecordBatchStream performs IPC serialization and compression after it calls
// RecordBatchReader::ReadNext().  Bind the entire callback so that work stays
// in the query's resource group as well as the SQL fetch and conversion.
class ObFlightSqlResultStream final : public arrow::flight::FlightDataStream
{
public:
  ObFlightSqlResultStream(const std::shared_ptr<ObFlightBatchReader> &reader,
                         const arrow::ipc::IpcWriteOptions &options,
                         const ObFlightReaderCtx &ctx)
      : reader_(reader),
        stream_(new arrow::flight::RecordBatchStream(reader, options)),
        tenant_id_(ctx.session->tenant_id),
        resource_group_id_(ctx.resource_group_id),
        session_(ctx.session->ob_session),
        timeout_ts_(ctx.timeout_ts)
  {}

  ~ObFlightSqlResultStream() override
  {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    share::ObSystemTenantCgroupGuard cgroup_guard;
    ObFlightSqlStreamExecutionScope execution_scope(session_, timeout_ts_);
    int ret = tenant_guard.switch_to(tenant_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("[FlightSQL] result stream destruction: tenant switch failed",
               K(ret), K_(tenant_id));
    } else if (OB_FAIL(cgroup_guard.switch_to(tenant_id_, resource_group_id_))) {
      LOG_WARN("[FlightSQL] result stream destruction: cgroup switch failed",
               K(ret), K_(tenant_id), K_(resource_group_id));
    }
    (void)Close();
    // Cached Arrow payloads must be released before their reader-owned pool.
    stream_.reset();
    reader_.reset();
  }

  std::shared_ptr<arrow::Schema> schema() override { return reader_->schema(); }

  arrow::Result<arrow::flight::FlightPayload> GetSchemaPayload() override
  {
    int ret = OB_SUCCESS;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    share::ObSystemTenantCgroupGuard cgroup_guard;
    ObFlightSqlStreamExecutionScope execution_scope(session_, timeout_ts_);
    if (OB_FAIL(tenant_guard.switch_to(tenant_id_))) {
      return ob_sql_exec_ret_to_arrow_status(ret, nullptr, "Flight result schema tenant");
    } else if (OB_FAIL(cgroup_guard.switch_to(tenant_id_, resource_group_id_))) {
      return ob_sql_exec_ret_to_arrow_status(ret, nullptr, "Flight result schema cgroup");
    } else if (OB_ISNULL(stream_)) {
      return arrow::Status::Invalid("Flight result stream is closed");
    }
    return stream_->GetSchemaPayload();
  }

  arrow::Result<arrow::flight::FlightPayload> Next() override
  {
    int ret = OB_SUCCESS;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    share::ObSystemTenantCgroupGuard cgroup_guard;
    ObFlightSqlStreamExecutionScope execution_scope(session_, timeout_ts_);
    if (OB_FAIL(tenant_guard.switch_to(tenant_id_))) {
      return ob_sql_exec_ret_to_arrow_status(ret, nullptr, "Flight result tenant");
    } else if (OB_FAIL(cgroup_guard.switch_to(tenant_id_, resource_group_id_))) {
      return ob_sql_exec_ret_to_arrow_status(ret, nullptr, "Flight result cgroup");
    } else if (OB_ISNULL(stream_)) {
      return arrow::flight::FlightPayload{};
    }
    return stream_->Next();
  }

  arrow::Status Close() override
  {
    if (OB_ISNULL(stream_)) {
      return arrow::Status::OK();
    }
    int ret = OB_SUCCESS;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    share::ObSystemTenantCgroupGuard cgroup_guard;
    ObFlightSqlStreamExecutionScope execution_scope(session_, timeout_ts_);
    if (OB_FAIL(tenant_guard.switch_to(tenant_id_))) {
      LOG_WARN("[FlightSQL] result stream close: tenant switch failed",
               K(ret), K_(tenant_id));
    } else if (OB_FAIL(cgroup_guard.switch_to(tenant_id_, resource_group_id_))) {
      LOG_WARN("[FlightSQL] result stream close: cgroup switch failed",
               K(ret), K_(tenant_id), K_(resource_group_id));
    }
    const arrow::Status status = stream_->Close();
    stream_.reset();
    // Keep reader_ (and its Arrow memory pool) until this stream is destroyed;
    // the transport may still hold a payload returned by Next().
    return OB_SUCC(ret) || !status.ok()
        ? status : ob_sql_exec_ret_to_arrow_status(ret, nullptr, "Flight result close");
  }

private:
  std::shared_ptr<ObFlightBatchReader> reader_;
  std::unique_ptr<arrow::flight::RecordBatchStream> stream_;
  uint64_t tenant_id_;
  uint64_t resource_group_id_;
  sql::ObSQLSessionInfo *session_;
  int64_t timeout_ts_;
  DISALLOW_COPY_AND_ASSIGN(ObFlightSqlResultStream);
};

// A failed DoGet open has no RecordBatchReader to write the final audit row.
// Record it here with the same trace ID returned to the Flight client.
static void flight_sql_audit_failed_query(
    ObFlightSqlSession &session,
    const ObFlightSqlQueryProcessor &proc,
    const std::string &sql_text,
    const int64_t request_time,
    const int query_ret,
    const char *request_name)
{
  if (OB_ISNULL(session.ob_session) || proc.trace_id().empty()) {
    return;
  }
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  const int tenant_ret = tenant_guard.switch_to(session.tenant_id);
  if (OB_SUCCESS != tenant_ret) {
    LOG_WARN_RET(tenant_ret, "[FlightSQL] failed query audit: tenant switch failed",
                 K(tenant_ret), K(session.tenant_id));
    return;
  }
  try {
    ObFlightSqlAuditRecord rec;
    const int64_t now = common::ObTimeUtility::current_time();
    rec.tenant_id = session.tenant_id;
    rec.resource_group_id = proc.resource_group_id();
    rec.plan_id = static_cast<int64_t>(proc.plan_id());
    rec.plan_type = proc.plan_type();
    rec.stmt_type = proc.stmt_type();
    rec.is_hit_plan_cache = proc.is_hit_plan_cache();
    rec.request_time = request_time;
    rec.elapsed_time = now > request_time ? now - request_time : 0;
    rec.execute_time = rec.elapsed_time;
    rec.ret_code = query_ret;
    rec.peer_port = session.peer_port;
    rec.ob_session_id = session.ob_session->get_server_sid();
#define FSQL_SAFE_COPY(dst, src) \
  do { strncpy((dst), (src).c_str(), sizeof(dst) - 1); (dst)[sizeof(dst) - 1] = '\0'; } while (0)
    FSQL_SAFE_COPY(rec.user_name, session.user_name);
    FSQL_SAFE_COPY(rec.peer_ip, session.peer_ip);
    FSQL_SAFE_COPY(rec.trace_id, proc.trace_id());
    FSQL_SAFE_COPY(rec.sql_id, proc.sql_id());
    {
      std::lock_guard<std::mutex> guard(session.mutex);
      FSQL_SAFE_COPY(rec.schema_name, session.current_schema);
      FSQL_SAFE_COPY(rec.catalog_name, session.current_catalog);
    }
#undef FSQL_SAFE_COPY
    rec.sql_text = sql_text;
    mirror_flight_sql_audit_record_to_gv_sql_audit(
        *session.ob_session, rec, request_name);
  } catch (const std::bad_alloc &) {
    // Preserve the original query error if audit allocation fails.
  } catch (const std::exception &e) {
    LOG_WARN_RET(query_ret, "[FlightSQL] failed query audit: unexpected exception",
                 "what", e.what());
  }
}

static bool flight_sql_schema_has_timestamp_ns(
    const std::shared_ptr<arrow::Schema> &arrow_schema)
{
  bool found = false;
  if (arrow_schema) {
    for (int i = 0; !found && i < arrow_schema->num_fields(); ++i) {
      const std::shared_ptr<arrow::Field> field = arrow_schema->field(i);
      if (field && field->type() && field->type()->id() == arrow::Type::TIMESTAMP) {
        const arrow::TimestampType &timestamp_type =
            static_cast<const arrow::TimestampType &>(*field->type());
        found = timestamp_type.unit() == arrow::TimeUnit::NANO;
      }
    }
  }
  return found;
}

static std::string flight_sql_ingest_error_message(
    const int ret, const std::shared_ptr<arrow::Schema> &arrow_schema)
{
  if (OB_NOT_SUPPORTED == ret && flight_sql_schema_has_timestamp_ns(arrow_schema)) {
    return
        "Flight SQL ingest does not support Arrow timestamp[ns]; "
        "cast it explicitly to timestamp[us], timestamp[ms], or timestamp[s]";
  }
  return std::string();
}

static std::string flight_sql_prepared_parameter_error_message(
    const int ret, const std::shared_ptr<arrow::Schema> &arrow_schema)
{
  if (OB_NOT_SUPPORTED == ret && flight_sql_schema_has_timestamp_ns(arrow_schema)) {
    return
        "Flight SQL prepared parameters do not support Arrow timestamp[ns]; "
        "cast it explicitly to timestamp[us], timestamp[ms], or timestamp[s]";
  }
  return std::string();
}

// ============================================================
// Session catalog / schema helpers
// Used by both Authenticate (header path) and SetSessionOptions (RPC path).
// ============================================================

// Apply catalog to session. Updates session.current_catalog on success.
static int apply_catalog_to_session(ObFlightSqlSession &session, const std::string &cat)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(
      common::ObMemAttr(session.tenant_id, "FlightSQLSess"),
      false /* use_500 */);
  if (OB_ISNULL(session.ob_session)) { return OB_ERR_UNEXPECTED; }
  if (0 == strcasecmp(cat.c_str(), OB_INTERNAL_CATALOG_NAME)) {
    ret = session.ob_session->set_internal_catalog_db(nullptr);
    if (OB_SUCC(ret)) { session.current_catalog = cat; }
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObCatalogSchema *catalog_schema = nullptr;
    if (OB_ISNULL(GCTX.schema_service_)) {
      return OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session.tenant_id, schema_guard))) {
      LOG_WARN("[FlightSQL] apply_catalog_to_session: get_tenant_schema_guard failed",
               K(ret), "catalog", cat.c_str());
    } else {
      const common::ObString cat_str(cat.size(), cat.c_str());
      if (OB_FAIL(schema_guard.get_catalog_schema_by_name(session.tenant_id, cat_str, catalog_schema))) {
        LOG_WARN("[FlightSQL] apply_catalog_to_session: get_catalog_schema_by_name failed",
                 K(ret), "catalog", cat.c_str());
      } else if (OB_ISNULL(catalog_schema)) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("[FlightSQL] apply_catalog_to_session: catalog not found",
                 K(ret), "catalog", cat.c_str());
      } else if (OB_FAIL(session.ob_session->set_default_catalog_db(
                     catalog_schema->get_catalog_id(),
                     common::OB_INVALID_ID,
                     common::ObString(""),
                     nullptr))) {
        LOG_WARN("[FlightSQL] apply_catalog_to_session: set_default_catalog_db failed",
                 K(ret), "catalog", cat.c_str());
      } else {
        session.current_catalog = cat;
      }
    }
  }
  return ret;
}

// Apply schema to session. Updates session.current_schema on success.
static int apply_schema_to_session(ObFlightSqlSession &session, const std::string &db)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(
      common::ObMemAttr(session.tenant_id, "FlightSQLSess"),
      false /* use_500 */);
  share::schema::ObSchemaGetterGuard schema_guard;
  ObSessionPrivInfo session_priv;
  const common::ObString db_str(db.size(), db.c_str());
  uint64_t catalog_id = OB_INVALID_ID;
  uint64_t db_id = OB_INVALID_ID;

  if (OB_ISNULL(session.ob_session) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] apply_schema_to_session: session or schema service is null",
             K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 session.ob_session->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("[FlightSQL] apply_schema_to_session: get schema guard failed",
             K(ret), "schema", db.c_str());
  } else if (OB_FAIL(session.ob_session->get_session_priv_info(session_priv))) {
    LOG_WARN("[FlightSQL] apply_schema_to_session: get session privilege failed",
             K(ret), "schema", db.c_str());
  } else if (OB_FALSE_IT(catalog_id = session.ob_session->get_current_default_catalog())) {
  } else if (OB_FAIL(schema_guard.check_db_access(
                 session_priv,
                 session.ob_session->get_enable_role_array(),
                 catalog_id,
                 db_str))) {
    if (OB_ERR_NO_DB_SELECTED == ret) {
      ret = OB_ERR_BAD_DATABASE;
    }
    LOG_WARN("[FlightSQL] apply_schema_to_session: check database access failed",
             K(ret), K(catalog_id), "schema", db.c_str());
  } else if (OB_FALSE_IT(db_id = is_internal_catalog_id(catalog_id)
                                    ? OB_INVALID_ID
                                    : OB_MIN_EXTERNAL_OBJECT_ID + 1)) {
  } else if (is_internal_catalog_id(catalog_id)
             && OB_FAIL(schema_guard.get_database_id(
                    session.ob_session->get_effective_tenant_id(), db_str, db_id))) {
    LOG_WARN("[FlightSQL] apply_schema_to_session: get database id failed",
             K(ret), K(catalog_id), "schema", db.c_str());
  } else if (OB_FAIL(session.ob_session->set_default_catalog_db(
                 catalog_id, db_id, db_str, nullptr))) {
    LOG_WARN("[FlightSQL] apply_schema_to_session: set catalog database failed",
             K(ret), K(catalog_id), K(db_id), "schema", db.c_str());
  } else if (OB_FAIL(session.ob_session->update_database_variables(&schema_guard))) {
    LOG_WARN("[FlightSQL] apply_schema_to_session: update database variables failed",
             K(ret), K(catalog_id), K(db_id), "schema", db.c_str());
  } else {
    session.ob_session->set_db_priv_set(session_priv.db_priv_set_);
    session.current_schema = db;
  }
  return ret;
}

// ============================================================
// Exception-safety macros — must be defined before any class that uses them.
// LOG_WARN requires 'ret' in scope; each catch block declares its own.
// ============================================================

// Simple catch — use for handlers that don't hold an active session query.
#define OB_FLIGHT_SQL_CATCH_ALL \
  catch (const std::bad_alloc &) { \
    /* avoid any heap allocation here — LOG_WARN itself may allocate */ \
    return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal, "OOM"); \
  } catch (const std::exception &e) { \
    int ret = OB_SUCCESS; \
    LOG_WARN("[FlightSQL] handler: unexpected exception", "what", e.what()); \
    return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal, \
                                          std::string("[FlightSQL] unexpected exception: ") + e.what()); \
  } catch (...) { \
    int ret = OB_SUCCESS; \
    LOG_WARN("[FlightSQL] handler: unknown exception"); \
    return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal, \
                                          "[FlightSQL] unknown exception"); \
  }

// Extended catch for query handlers (GetFlightInfoStatement, DoGetStatement).
// Removes the session on exception so the client must re-authenticate rather
// than reuse a session whose internal state (ObSQLSessionInfo, open
// transactions, etc.) is unknown.
#define OB_FLIGHT_SQL_CATCH_AND_REMOVE_SESSION \
  catch (const std::bad_alloc &) { \
    /* avoid any heap allocation here — LOG_WARN itself may allocate */ \
    session_mgr_.remove(context.peer_identity()); \
    return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal, "OOM"); \
  } catch (const std::exception &e) { \
    int ret = OB_SUCCESS; \
    session_mgr_.remove(context.peer_identity()); \
    LOG_WARN("[FlightSQL] query handler: unexpected exception, session removed", "what", e.what()); \
    return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal, \
                                          std::string("[FlightSQL] unexpected exception: ") + e.what()); \
  } catch (...) { \
    int ret = OB_SUCCESS; \
    session_mgr_.remove(context.peer_identity()); \
    LOG_WARN("[FlightSQL] query handler: unknown exception, session removed"); \
    return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal, \
                                          "[FlightSQL] unknown exception"); \
  }

// CancelQuery returns CancelResult, not FlightStatus — map exceptions to kUnspecified.
#define OB_FLIGHT_SQL_CATCH_CANCEL_QUERY \
  catch (const std::bad_alloc &) { \
    return arrow::flight::sql::CancelResult::kUnspecified; \
  } catch (const std::exception &e) { \
    int ret = OB_SUCCESS; \
    LOG_WARN("[FlightSQL] CancelQuery: unexpected exception", "what", e.what()); \
    return arrow::flight::sql::CancelResult::kUnspecified; \
  } catch (...) { \
    int ret = OB_SUCCESS; \
    LOG_WARN("[FlightSQL] CancelQuery: unknown exception"); \
    return arrow::flight::sql::CancelResult::kUnspecified; \
  }

// Listener initialization crosses STL and Arrow allocation boundaries.  Do not
// log from these catch blocks: the caller logs the returned OceanBase error,
// and avoiding logging here keeps OOM handling allocation-free and secret-free.
#define OB_FLIGHT_SQL_CATCH_INIT_LISTENER \
  catch (const std::bad_alloc &) { \
    return OB_ALLOCATE_MEMORY_FAILED; \
  } catch (const std::exception &) { \
    return OB_ERR_UNEXPECTED; \
  } catch (...) { \
    return OB_ERR_UNEXPECTED; \
  }

// ============================================================
// Auth handler
// ============================================================

class ObFlightSqlAuthHandler : public arrow::flight::ServerAuthHandler
{
public:
  explicit ObFlightSqlAuthHandler(ObFlightSqlSessionManager *mgr) : session_mgr_(mgr) {}
  ~ObFlightSqlAuthHandler() override = default;

  // Parse username@tenant:password from the client's BasicAuth proto.
  // Create a ObFlightSqlSession and return its UUID as the bearer token.
  arrow::Status Authenticate(const arrow::flight::ServerCallContext &context,
                             arrow::flight::ServerAuthSender *outgoing,
                             arrow::flight::ServerAuthReader *incoming) override
  {
    try {
    // Give this gRPC thread a real trace ID so LOG_WARN lines and error messages
    // are searchable. gRPC threads default to trace_id=0; we init here the same
    // way ObMPBase does for MySQL connections.
    common::ObCurTraceId::init(GCTX.self_addr());
    std::string raw;
    std::string user_name, tenant_name, password;
    DEFER(flight_sql_cleanse_string(raw));
    DEFER(flight_sql_cleanse_string(password));
    const char *cred_source = "none";
    int ret = OB_SUCCESS;
    // Path 1: Handshake stream payload (BasicAuth proto or raw "user@tenant:pass")
    const arrow::Status read_st = incoming->Read(&raw);
    LOG_TRACE("[FlightSQL] Authenticate: stream read",
             "ok", read_st.ok(), "payload_bytes", (int64_t)raw.size());
    if (read_st.ok() && !raw.empty()) {
      arrow::flight::BasicAuth auth;
      DEFER(flight_sql_cleanse_string(auth.password));
      const arrow::Status parse_st = arrow::flight::BasicAuth::Deserialize(raw, &auth);
      if (parse_st.ok() && !auth.username.empty()) {
        parse_user_tenant_(auth.username, user_name, tenant_name);
        password = auth.password;
        cred_source = "BasicAuth-proto";
      } else {
        const size_t colon = raw.find(':');
        const std::string at_user = (colon != std::string::npos) ? raw.substr(0, colon) : raw;
        password = (colon != std::string::npos) ? raw.substr(colon + 1) : "";
        parse_user_tenant_(at_user, user_name, tenant_name);
        cred_source = "raw-string";
      }
    }

    // Path 2: ADBC C++ driver sends "Authorization: Basic base64(user:pass)" as
    // gRPC call metadata instead of using the Handshake stream payload.
    if (user_name.empty()) {
      std::string basic_user;
      const bool found = extract_basic_credentials_(context.incoming_headers(), basic_user, password);
      LOG_TRACE("[FlightSQL] Authenticate: Basic header extraction", K(ret),
               K(found), "raw_user", basic_user.c_str());
      if (found) {
        parse_user_tenant_(basic_user, user_name, tenant_name);
        cred_source = "Basic-header";
      }
    }

    // Reject handshake when no username was supplied.
    if (user_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] Authenticate: rejected (no username in handshake payload "
               "or Authorization: Basic)",
               K(ret));
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          "Flight SQL requires a username: use BasicAuth in the handshake payload "
          "or send \"Authorization: Basic\" with user@tenant (and password) "
          "in call metadata.");
    }

    LOG_TRACE("[FlightSQL] Authenticate: credentials resolved",
             "source", cred_source,
             "user", user_name.c_str(),
             "tenant", tenant_name.c_str());

    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    bool is_oracle_mode = false;
    ARROW_RETURN_NOT_OK(resolve_tenant_(tenant_name, tenant_id, is_oracle_mode));
    ret = OB_SUCCESS;

    // Parse gRPC peer address ("ipv4:1.2.3.4:5678" or "1.2.3.4:5678")
    std::string peer_ip;
    int32_t peer_port = 0;
    {
      std::string peer = context.peer();
      if (peer.substr(0, 5) == "ipv4:") { peer = peer.substr(5); }
      else if (peer.substr(0, 5) == "ipv6:") { peer = peer.substr(5); }
      const size_t last_colon = peer.rfind(':');
      if (last_colon != std::string::npos) {
        peer_ip   = peer.substr(0, last_colon);
        peer_port = std::stoi(peer.substr(last_colon + 1));
      } else {
        peer_ip = peer;
      }
    }

    // Pre-check: reject immediately if this observer has no unit for the tenant.
    // The client should query oceanbase.__all_unit to find the right observer.
    if (OB_NOT_NULL(GCTX.omt_) && !GCTX.omt_->has_tenant(tenant_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] Authenticate: tenant is remote", K(ret), "tenant", tenant_name.c_str(),
               K(tenant_id));
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unavailable,
          "tenant '" + tenant_name + "' has no unit on this observer;"
          " query oceanbase.__all_unit to find the right observer");
    }

    // Create session
    int session_ret = OB_SUCCESS;
    const char *create_trace_id = common::ObCurTraceId::get_trace_id_str();
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_->create(
        tenant_id, tenant_name, user_name, password, is_oracle_mode,
        peer_ip, peer_port, &session_ret);
    flight_sql_cleanse_string(password);
    flight_sql_cleanse_string(raw);
    if (!session) {
      return ob_ret_to_flight_status(session_ret, tenant_name, create_trace_id);
    }

    LOG_TRACE("[FlightSQL] Authenticate: session created",
             "session_id", session->session_id.c_str(),
             "tenant_id", static_cast<int64_t>(tenant_id));

    // Apply catalog and schema from gRPC call headers supplied by the client before
    // the handshake (mirrors PostgreSQL StartupMessage database field).
    // Headers: "x-ob-catalog", "x-ob-schema" — catalog is always applied before schema.
    {
      const arrow::flight::CallHeaders &hdrs = context.incoming_headers();
      std::string init_catalog, init_schema;
      arrow::flight::CallHeaders::const_iterator cat_it = hdrs.find(FLIGHT_SQL_HEADER_CATALOG);
      arrow::flight::CallHeaders::const_iterator db_it  = hdrs.find(FLIGHT_SQL_HEADER_SCHEMA);
      if (cat_it != hdrs.end()) { init_catalog = std::string(cat_it->second); }
      if (db_it  != hdrs.end()) { init_schema  = std::string(db_it->second);  }

      if (!init_catalog.empty()) {
        const int cat_ret = apply_catalog_to_session(*session, init_catalog);
        if (OB_SUCCESS != cat_ret) {
          LOG_WARN("[FlightSQL] Authenticate: invalid catalog in x-ob-catalog header, session removed",
                   K(cat_ret), "catalog", init_catalog.c_str(),
                   "session_id", session->session_id.c_str());
          session_mgr_->remove(session->session_id);
          return arrow::flight::MakeFlightError(
              arrow::flight::FlightStatusCode::Unauthenticated,
              "invalid catalog '" + init_catalog + "' (OB-" + std::to_string(-cat_ret) + ");"
              " session has been closed, please re-authenticate");
        }
        LOG_TRACE("[FlightSQL] Authenticate: catalog set from header",
                 "catalog", init_catalog.c_str());
      }
      if (!init_schema.empty()) {
        const int db_ret = apply_schema_to_session(*session, init_schema);
        if (OB_SUCCESS != db_ret) {
          const std::string catalog_ctx = session->current_catalog;
          LOG_WARN("[FlightSQL] Authenticate: invalid schema in x-ob-schema header, session removed",
                   K(db_ret), "schema", init_schema.c_str(),
                   "catalog", catalog_ctx.c_str(),
                   "session_id", session->session_id.c_str());
          session_mgr_->remove(session->session_id);
          return arrow::flight::MakeFlightError(
              arrow::flight::FlightStatusCode::Unauthenticated,
              "invalid schema '" + init_schema + "' in catalog '" + catalog_ctx
              + "' (OB-" + std::to_string(-db_ret) + ");"
              " session has been closed, please re-authenticate");
        }
        LOG_TRACE("[FlightSQL] Authenticate: schema set from header",
                 "schema", init_schema.c_str());
      }
    }

    // ADBC clients expect the token as an "authorization: Bearer <token>" response header.
    // Standard Handshake clients read it from the stream payload.
    context.AddHeader(FLIGHT_SQL_HEADER_AUTHORIZATION, "Bearer " + session->session_id);
    context.AddHeader(FLIGHT_SQL_HEADER_SESSION_ID, std::to_string(session->free_ctx.sessid_));
    return outgoing->Write(session->session_id);
    } OB_FLIGHT_SQL_CATCH_ALL
  }

  // Validate the bearer token (= session_id UUID) on every subsequent RPC.
  arrow::Status IsValid(const arrow::flight::ServerCallContext &context,
                        const std::string &token,
                        std::string *peer_identity) override
  {
    try {
    // Arrow Flight calls IsValid with the token from the Handshake stream.
    // ADBC C++ clients instead send "Authorization: Bearer <token>" as raw gRPC
    // call metadata, which Arrow Flight does not automatically route to `token`.
    // Fall back to reading the header directly.
    std::string actual_token = token;
    const char *token_source = "stream";
    int ret = OB_SUCCESS;
    if (actual_token.empty()) {
      actual_token = extract_bearer_token_(context.incoming_headers());
      token_source = "Bearer-header";
    }
    LOG_TRACE("[FlightSQL] IsValid",
             "token_source", token_source,
             "token_prefix", actual_token.substr(0, std::min(size_t(8), actual_token.size())).c_str(),
             "token_empty", actual_token.empty());
    if (actual_token.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] IsValid: no token in stream or headers", K(ret));
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated,
                                            "missing auth token");
    }
    const std::shared_ptr<ObFlightSqlSession> session = session_mgr_->get(actual_token);
    if (!session) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] IsValid: session not found",
               K(ret),
               "token_prefix", actual_token.substr(0, std::min(size_t(8), actual_token.size())).c_str());
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated,
                                            "session not found or expired: " + actual_token);
    }
    LOG_TRACE("[FlightSQL] IsValid: OK",
             "session_id_prefix", actual_token.substr(0, std::min(size_t(8), actual_token.size())).c_str(),
             "user", session->user_name.c_str(),
             "tenant", session->tenant_name.c_str());
    if (OB_NOT_NULL(peer_identity)) {
      *peer_identity = actual_token;
    }
    ret = OB_SUCCESS;
    return arrow::Status::OK();
    } OB_FLIGHT_SQL_CATCH_ALL
  }

private:
  // Extract "Bearer <token>" → "<token>" from gRPC call metadata headers.
  static std::string extract_bearer_token_(const arrow::flight::CallHeaders &hdrs)
  {
    arrow::flight::CallHeaders::const_iterator it = hdrs.find(FLIGHT_SQL_HEADER_AUTHORIZATION);
    if (it == hdrs.end()) { return {}; }
    const std::string_view val = it->second;
    static constexpr std::string_view kBearer = "Bearer ";
    if (val.size() > kBearer.size() && val.substr(0, kBearer.size()) == kBearer) {
      return std::string(val.substr(kBearer.size()));
    }
    return {};
  }

  // Extract username and password from "Authorization: Basic base64(user:pass)".
  static bool extract_basic_credentials_(const arrow::flight::CallHeaders &hdrs,
                                         std::string &user_out,
                                         std::string &password_out)
  {
    bool found = false;
    user_out.clear();
    password_out.clear();
    arrow::flight::CallHeaders::const_iterator it = hdrs.find(FLIGHT_SQL_HEADER_AUTHORIZATION);
    if (it != hdrs.end()) {
      const std::string_view val = it->second;
      static constexpr std::string_view kBasic = "Basic ";
      if (val.size() > kBasic.size() && val.substr(0, kBasic.size()) == kBasic) {
        std::string decoded = arrow::util::base64_decode(val.substr(kBasic.size()));
        DEFER(flight_sql_cleanse_string(decoded));
        const size_t colon = decoded.find(':');
        user_out = (colon != std::string::npos) ? decoded.substr(0, colon) : decoded;
        password_out = (colon != std::string::npos) ? decoded.substr(colon + 1) : "";
        found = !user_out.empty();
      }
    }
    return found;
  }

  static void parse_user_tenant_(const std::string &at_user,
                                 std::string &user_out,
                                 std::string &tenant_out)
  {
    const size_t at = at_user.rfind('@');
    if (at != std::string::npos) {
      user_out   = at_user.substr(0, at);
      tenant_out = at_user.substr(at + 1);
    } else {
      user_out   = at_user;
      tenant_out = "";
    }
  }

  static arrow::Status resolve_tenant_(const std::string &tenant_name,
                                       uint64_t &tenant_id_out,
                                       bool &is_oracle_mode_out)
  {
    int ret = OB_SUCCESS;
    tenant_id_out = OB_INVALID_TENANT_ID;
    is_oracle_mode_out = false;

    if (tenant_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] authenticate: tenant missing from username (expected user@tenant)",
               K(ret));
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          "Flight SQL username must include tenant name, e.g. user@my_tenant");
    }

    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] resolve_tenant: schema_service is null", K(ret));
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal,
                                            "schema service unavailable");
    }

    share::schema::ObSchemaGetterGuard sys_guard;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, sys_guard))) {
      LOG_WARN("[FlightSQL] resolve_tenant: get sys tenant schema guard failed", K(ret));
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal,
                                            "failed to load tenant catalog");
    }

    uint64_t tid = OB_INVALID_TENANT_ID;
    const common::ObString name_str(tenant_name.size(), tenant_name.c_str());
    ret = sys_guard.get_tenant_id(name_str, tid);
    if (OB_FAIL(ret)) {
      LOG_WARN("[FlightSQL] resolve_tenant: get_tenant_id failed",
               K(ret), "tenant", tenant_name.c_str());
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal,
                                            "failed to resolve tenant name");
    }
    if (OB_INVALID_TENANT_ID == tid) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] resolve_tenant: tenant not found", K(ret),
               "tenant", tenant_name.c_str());
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          "invalid username or password");
    }
    tenant_id_out = tid;

    share::schema::ObSchemaGetterGuard tenant_guard;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tid, tenant_guard))) {
      LOG_WARN("[FlightSQL] resolve_tenant: get_tenant_schema_guard failed", K(ret), K(tid));
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal,
                                            "failed to load tenant schema guard");
    }
    const share::schema::ObTenantSchema *ts = nullptr;
    if (OB_FAIL(tenant_guard.get_tenant_info(tid, ts))) {
      LOG_WARN("[FlightSQL] resolve_tenant: get_tenant_info failed", K(ret), K(tid));
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal,
                                            "failed to resolve tenant compatibility mode");
    }
    if (OB_ISNULL(ts)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] resolve_tenant: tenant schema is NULL", K(ret), K(tid));
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Internal,
                                            "tenant schema unavailable");
    }
    is_oracle_mode_out = ts->is_oracle_tenant();
    return arrow::Status::OK();
  }

  ObFlightSqlSessionManager *session_mgr_;
};

// Interrupt only the SQL statement. Transaction completion remains explicit.
static int flight_sql_interrupt_session(ObFlightSqlSession &session)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] cancellation tenant switch failed", K(ret));
  } else if (OB_ISNULL(GCTX.session_mgr_) || OB_ISNULL(session.ob_session)) {
    ret = OB_NOT_INIT;
  } else {
    sql::ObSQLSessionInfo *sql_session = nullptr;
    sql::ObSessionGetterGuard guard(*GCTX.session_mgr_, session.free_ctx.sessid_);
    if (OB_FAIL(guard.get_session(sql_session))) {
      LOG_WARN("[FlightSQL] cancellation session lookup failed", K(ret));
    } else if (OB_ISNULL(sql_session)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(GCTX.session_mgr_->kill_query(*sql_session))) {
      LOG_WARN("[FlightSQL] SQL cancellation failed", K(ret));
    }
  }
  return ret;
}

// Construct after the query guard: retire fences all cancellation callbacks
// before that guard can unlock, so a captured handle cannot kill the next SQL.
class ObFlightSqlDoPutCancellationScope
{
  struct State {
    std::shared_ptr<ObFlightSqlRpcCancelHandle> rpc;
    ObFlightSqlSession *session;
    bool interrupt_sql;
    std::mutex mutex;
    std::string ingest_key;

    void abort_ingest()
    {
      std::string key;
      {
        std::lock_guard<std::mutex> guard(mutex);
        key = ingest_key;
      }
      if (!key.empty()) {
        MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
        if (OB_SUCCESS == tenant_guard.switch_to(session->tenant_id)) {
          (void)ObFlightSqlIngestRegistry::instance().abort(key);
        }
      }
    }
    void cancel()
    {
      rpc->cancel();
      if (interrupt_sql) { (void)flight_sql_interrupt_session(*session); }
      abort_ingest();
    }
  };
public:
  ObFlightSqlDoPutCancellationScope(ObFlightSqlSession &session,
      const arrow::flight::ServerCallContext &context,
      const ObFlightSqlRpcCancelRegistry &registry, bool interrupt_sql = true)
      : session_(session), context_(context)
  {
    const auto header = context.incoming_headers().find(kFlightSqlRpcCancelIdHeader);
    if (header != context.incoming_headers().end()) {
      auto rpc = registry.get(std::string(header->second), header->second.data());
      if (rpc) {
        state_ = std::make_shared<State>();
        state_->rpc = std::move(rpc);
        state_->session = &session;
        state_->interrupt_sql = interrupt_sql;
        auto state = state_;
        request_ = std::make_shared<ObFlightSqlRequestCancellation>(
            [state] { state->cancel(); });
        std::lock_guard<std::mutex> guard(session_.mutex);
        if (!session_.closing_) {
          session_.active_doput_cancellation = request_;
          pending_cancel_ = session_.doput_cancel_pending_;
        } else {
          request_.reset();
        }
      }
    }
  }
  ~ObFlightSqlDoPutCancellationScope() { (void)finish(); }

  const std::shared_ptr<ObFlightSqlRequestCancellation> &request() const { return request_; }
  void activate()
  {
    if (pending_cancel_ && request_) { (void)request_->cancel([] {}); }
  }
  bool is_cancelled() const
  { return context_.is_cancelled() || (request_ && request_->is_cancelled()); }

  // A multistream DoPut also waits on an application condition variable.
  // Publishing its key races safely with cancellation before the wait starts.
  void set_ingest_key(const std::string &key)
  {
    {
      std::lock_guard<std::mutex> guard(state_->mutex);
      state_->ingest_key = key;
    }
    if (is_cancelled()) { state_->abort_ingest(); }
  }

  bool finish()
  {
    bool cancelled = context_.is_cancelled();
    if (request_) {
      cancelled = request_->retire([&] {
        // A kill while waiting for input has no SQL state scope to clear it.
        // Only clear QUERY_KILLED, never a killed session or deadlock state.
        if (state_->interrupt_sql && request_->is_cancelled() && OB_NOT_NULL(session_.ob_session)
            && sql::QUERY_KILLED == session_.ob_session->get_session_state()) {
          session_.ob_session->set_session_sleep();
        }
      }) || cancelled;
      std::lock_guard<std::mutex> guard(session_.mutex);
      if (session_.active_doput_cancellation == request_) {
        session_.active_doput_cancellation.reset();
      }
    }
    return cancelled;
  }
private:
  ObFlightSqlSession &session_;
  const arrow::flight::ServerCallContext &context_;
  std::shared_ptr<State> state_;
  std::shared_ptr<ObFlightSqlRequestCancellation> request_;
  bool pending_cancel_{false};
};

static arrow::Status flight_sql_doput_cancelled()
{
  return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Cancelled,
      "[FlightSQL] DoPut was cancelled");
}

struct ObFlightSqlBuilderHook
{
  int max_threads;
  int min_pollers;
  int max_pollers;
  ObFlightSqlRpcCancelRegistry *rpc_cancel_registry;
  void operator()(void *raw_builder) const
  {
    grpc::ServerBuilder *builder = static_cast<grpc::ServerBuilder *>(raw_builder);
    if (max_threads > 0) {
      grpc::ResourceQuota quota;
      quota.SetMaxThreads(max_threads);
      builder->SetResourceQuota(quota);
    }
    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> factories;
    factories.push_back(rpc_cancel_registry->make_factory());
    builder->experimental().SetInterceptorCreators(std::move(factories));
    builder->SetSyncServerOption(grpc::ServerBuilder::MIN_POLLERS, min_pollers);
    builder->SetSyncServerOption(grpc::ServerBuilder::MAX_POLLERS, max_pollers);
  }
};

} // anonymous namespace

// ============================================================
// V2 ingest: JSON helpers (for controlled JSON generated by the Python client)
// ============================================================

static std::string ob_json_str(const std::string &json, const std::string &key)
{
  const std::string search = "\"" + key + "\"";
  size_t pos = json.find(search);
  if (pos == std::string::npos) { return ""; }
  pos = json.find(':', pos + search.size());
  if (pos == std::string::npos) { return ""; }
  pos = json.find('"', pos);
  if (pos == std::string::npos) { return ""; }
  ++pos;
  size_t end = json.find('"', pos);
  if (end == std::string::npos) { return ""; }
  return json.substr(pos, end - pos);
}

static std::vector<std::string> ob_json_str_array(const std::string &json, const std::string &key)
{
  std::vector<std::string> result;
  const std::string search = "\"" + key + "\"";
  size_t pos = json.find(search);
  if (pos == std::string::npos) { return result; }
  pos = json.find('[', pos + search.size());
  if (pos == std::string::npos) { return result; }
  ++pos;
  while (pos < json.size() && json[pos] != ']') {
    pos = json.find('"', pos);
    if (pos == std::string::npos) { break; }
    ++pos;
    size_t end = json.find('"', pos);
    if (end == std::string::npos) { break; }
    result.push_back(json.substr(pos, end - pos));
    pos = end + 1;
  }
  return result;
}

static int64_t ob_json_int(const std::string &json, const std::string &key, int64_t def = 0)
{
  const std::string search = "\"" + key + "\"";
  size_t pos = json.find(search);
  if (pos == std::string::npos) { return def; }
  pos = json.find(':', pos + search.size());
  if (pos == std::string::npos) { return def; }
  ++pos;
  while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) { ++pos; }
  if (pos >= json.size()) { return def; }
  try { return std::stoll(json.substr(pos)); } catch (...) { return def; }
}

static bool ob_json_has_key(const std::string &json, const std::string &key)
{
  return json.find("\"" + key + "\"") != std::string::npos;
}

static bool ob_json_bool(const std::string &json, const std::string &key, bool def = false)
{
  const std::string search = "\"" + key + "\"";
  size_t pos = json.find(search);
  if (pos == std::string::npos) { return def; }
  pos = json.find(':', pos + search.size());
  if (pos == std::string::npos) { return def; }
  ++pos;
  while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) { ++pos; }
  if (json.compare(pos, 4, "true") == 0) { return true; }
  if (json.compare(pos, 5, "false") == 0) { return false; }
  return def;
}

// ExecuteIngest is a controlled internal protocol, but keep this bounded: a
// schema is metadata, not a RecordBatch payload.  Arrow's IPC Schema message
// preserves all field type details needed by the single catalog mapping.
static arrow::Result<std::shared_ptr<arrow::Schema>> ob_decode_ingest_arrow_schema(
    const std::string &encoded_schema)
{
  if (encoded_schema.empty()) {
    return arrow::Status::Invalid("[FlightSQL] Arrow schema is required for multi-stream ingest");
  }
  static constexpr size_t MAX_ENCODED_SCHEMA_BYTES = 4U * 1024U * 1024U;
  if (encoded_schema.size() > MAX_ENCODED_SCHEMA_BYTES) {
    return arrow::Status::Invalid("[FlightSQL] Arrow schema metadata is too large");
  }
  const std::string decoded_schema = arrow::util::base64_decode(encoded_schema);
  if (decoded_schema.empty()) {
    return arrow::Status::Invalid("[FlightSQL] invalid base64 Arrow schema metadata");
  }
  std::unique_ptr<arrow::io::BufferReader> input =
      arrow::io::BufferReader::FromString(decoded_schema);
  arrow::ipc::DictionaryMemo dictionary_memo;
  return arrow::ipc::ReadSchema(input.get(), &dictionary_memo);
}

// Build a one-row FlightDataStream for a V2 ingest result.
// action: "prepare_ingest" | "execute_ingest" | "cancel_ingest"
// result_json: {"ingest_id":"..."} or {"rows":N} or {}
static arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
ob_make_ingest_result_stream(const std::string &action, const std::string &result_json)
{
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::RecordBatch> batch;

  if (action == "prepare_ingest") {
    const std::string ingest_id = ob_json_str(result_json, "ingest_id");
    schema = arrow::schema({arrow::field("ingest_id", arrow::utf8())});
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr,
        arrow::MakeArrayFromScalar(arrow::StringScalar(ingest_id), 1));
    batch = arrow::RecordBatch::Make(schema, 1, {arr});
  } else if (action == "execute_ingest") {
    const int64_t rows = ob_json_int(result_json, "rows", 0);
    schema = arrow::schema({arrow::field("rows", arrow::int64())});
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr,
        arrow::MakeArrayFromScalar(arrow::Int64Scalar(rows), 1));
    batch = arrow::RecordBatch::Make(schema, 1, {arr});
  } else {
    schema = arrow::schema({arrow::field("status", arrow::utf8())});
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr,
        arrow::MakeArrayFromScalar(arrow::StringScalar("ok"), 1));
    batch = arrow::RecordBatch::Make(schema, 1, {arr});
  }

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatchReader> reader,
      arrow::RecordBatchReader::Make({batch}, schema));
  return std::make_unique<arrow::flight::RecordBatchStream>(reader);
}

// Schema returned by GetFlightInfoStatement for V2/V3 ingest actions.
static std::shared_ptr<arrow::Schema> ob_ingest_action_schema(const std::string &action)
{
  if (action == "prepare_ingest") {
    return arrow::schema({arrow::field("ingest_id", arrow::utf8())});
  } else if (action == "execute_ingest") {
    return arrow::schema({arrow::field("rows", arrow::int64())});
  }
  return arrow::schema({arrow::field("status", arrow::utf8())});
}

// ============================================================
// Server implementation
// ============================================================

struct ObOceanBaseFlightSqlServerImpl : public arrow::flight::sql::FlightSqlServerBase
{
  explicit ObOceanBaseFlightSqlServerImpl(share::ObGlobalContext *gctx)
      : arrow::flight::sql::FlightSqlServerBase(),
        gctx_(gctx)
  {
    using Opts = arrow::flight::sql::SqlInfoOptions;
    using Res  = arrow::flight::sql::SqlInfoResult;

    // ── Server metadata ───────────────────────────────────────────────────
    RegisterSqlInfo(Opts::FLIGHT_SQL_SERVER_NAME,          Res{std::string("OceanBase")});
    // Flight SQL protocol version implemented by this server (e.g. "1.0.0")
    RegisterSqlInfo(Opts::FLIGHT_SQL_SERVER_VERSION,
                    Res{std::string(kObFlightSqlImplVersion)});
    RegisterSqlInfo(Opts::FLIGHT_SQL_SERVER_ARROW_VERSION, Res{std::string("20.0.0")});
    RegisterSqlInfo(Opts::FLIGHT_SQL_SERVER_READ_ONLY,     Res{false});
    RegisterSqlInfo(Opts::FLIGHT_SQL_SERVER_SQL,           Res{true});
    RegisterSqlInfo(Opts::FLIGHT_SQL_SERVER_SUBSTRAIT,     Res{false});
    RegisterSqlInfo(Opts::FLIGHT_SQL_SERVER_TRANSACTION,
                    Res{int32_t(Opts::SQL_SUPPORTED_TRANSACTION_NONE)});
    RegisterSqlInfo(Opts::FLIGHT_SQL_SERVER_CANCEL,        Res{true});
    // OceanBase-specific extensions (vendor range ≥ 10000):
    //   10000 = OceanBase cluster version (e.g. "4.6.0.1")
    //   10001 = optimizer_features_enable_version
    // there are changes that the cluster version can not be got at this time
    // client should always use Opts::FLIGHT_SQL_SERVER_VERSION
    RegisterSqlInfo(kObClusterVersion,
                    Res{ob_format_version(GET_MIN_CLUSTER_VERSION())});
    RegisterSqlInfo(kObSqlInfoOptFeatureVersion,
                    Res{ob_format_version(DATA_CURRENT_VERSION)});

    // ── DDL capabilities ─────────────────────────────────────────────────
    RegisterSqlInfo(Opts::SQL_DDL_CATALOG, Res{false});
    RegisterSqlInfo(Opts::SQL_DDL_SCHEMA,  Res{true});
    RegisterSqlInfo(Opts::SQL_DDL_TABLE,   Res{true});

    // ── Identifier conventions (MySQL mode) ──────────────────────────────
    RegisterSqlInfo(Opts::SQL_IDENTIFIER_CASE,
                    Res{int32_t(Opts::SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)});
    RegisterSqlInfo(Opts::SQL_IDENTIFIER_QUOTE_CHAR, Res{std::string("`")});
    RegisterSqlInfo(Opts::SQL_QUOTED_IDENTIFIER_CASE,
                    Res{int32_t(Opts::SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)});

    // ── NULL behavior ─────────────────────────────────────────────────────
    RegisterSqlInfo(Opts::SQL_NULL_ORDERING,
                    Res{int32_t(Opts::SQL_NULLS_SORTED_LOW)});
    RegisterSqlInfo(Opts::SQL_NULL_PLUS_NULL_IS_NULL, Res{true});

    // ── Outer joins (SqlOuterJoinsSupportLevel: 0=none, 1=limited, 2=full) ──
    RegisterSqlInfo(Opts::SQL_OUTER_JOINS_SUPPORT_LEVEL, Res{int32_t(2)});

    // ── Schema / catalog terms ────────────────────────────────────────────
    RegisterSqlInfo(Opts::SQL_SCHEMA_TERM,    Res{std::string("database")});
    RegisterSqlInfo(Opts::SQL_PROCEDURE_TERM, Res{std::string("procedure")});
    RegisterSqlInfo(Opts::SQL_CATALOG_TERM,   Res{std::string("")});

    // ── Identifier length limits ──────────────────────────────────────────
    RegisterSqlInfo(Opts::SQL_MAX_COLUMN_NAME_LENGTH, Res{int64_t(64)});
    RegisterSqlInfo(Opts::SQL_MAX_TABLE_NAME_LENGTH,  Res{int64_t(64)});
    RegisterSqlInfo(Opts::SQL_MAX_USERNAME_LENGTH,    Res{int64_t(64)});

    // ── Transaction support ──────────────────────────────────────────────
    RegisterSqlInfo(Opts::SQL_TRANSACTIONS_SUPPORTED, Res{true});
    // Bitmask: bit i set means isolation level i supported.
    // Levels (SqlTransactionIsolationLevel): READ_COMMITTED=2, REPEATABLE_READ=3, SERIALIZABLE=4
    // → (1<<2)|(1<<3)|(1<<4) = 4|8|16 = 28
    RegisterSqlInfo(Opts::SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS,
                    Res{int64_t(4 | 8 | 16)});
    RegisterSqlInfo(Opts::SQL_DEFAULT_TRANSACTION_ISOLATION, Res{int64_t(3)});
    RegisterSqlInfo(Opts::SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT, Res{false});
    RegisterSqlInfo(Opts::SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED,  Res{false});

    int ret = OB_SUCCESS;
    if (OB_FAIL(session_mgr_.start())) {
      LOG_ERROR("[FlightSQL] session_mgr start failed", K(ret));
    } else if (OB_ISNULL(gctx_)) {
      ret = OB_ERR_UNEXPECTED;
      session_mgr_.stop();
      LOG_ERROR("[FlightSQL] global context is null", K(ret));
    } else {
      gctx_->flight_sql_session_mgr_ = &session_mgr_;
      LOG_TRACE("[FlightSQL] Server impl created");
    }
  }

  ~ObOceanBaseFlightSqlServerImpl() override
  {
    if (OB_NOT_NULL(gctx_)
        && gctx_->flight_sql_session_mgr_ == &session_mgr_) {
      gctx_->flight_sql_session_mgr_ = nullptr;
    }
    session_mgr_.stop();
    LOG_TRACE("[FlightSQL] Server impl destroyed");
  }

  // ============================================================
  // Adhoc query: GetFlightInfoStatement + DoGetStatement
  // ============================================================

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoStatement(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::StatementQuery &command,
      const arrow::flight::FlightDescriptor &descriptor) override
  {
    try {
    LOG_TRACE("[FlightSQL] GetFlightInfoStatement",
             "peer", context.peer().c_str(),
             "sql_preview", command.query.substr(0, std::min(size_t(100), command.query.size())).c_str());

    const std::string session_id = context.peer_identity();
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated,
                                            std::string("session not found: ") + session_id);
    }

    // ── V2 ingest: JSON ob_action body ─────────────────────────────────────
    if (!command.query.empty() && command.query[0] == '{') {
      const std::string action = ob_json_str(command.query, "ob_action");
      if (!action.empty()) {
        // Store the full body in pending_sqls so DoGetStatement can execute it.
        const std::string query_id = next_query_id();
        const std::string marker = std::string(OB_INGEST_ACTION_MARKER)
            + action + ":" + command.query;
        {
          lib::ObMallocHookAttrGuard malloc_guard(
              common::ObMemAttr(session->tenant_id, "FlightSQLSess"),
              false /* use_500 */);
          std::lock_guard<std::mutex> guard(session->mutex);
          session->pending_sqls[query_id] = marker;
        }
        const std::string handle = session_id + ":" + query_id;
        ARROW_ASSIGN_OR_RAISE(arrow::flight::Ticket ticket,
                              arrow::flight::sql::CreateStatementQueryTicket(handle));
        std::shared_ptr<arrow::Schema> schema = ob_ingest_action_schema(action);
        std::vector<arrow::flight::FlightEndpoint> endpoints{
            arrow::flight::FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};
        ARROW_ASSIGN_OR_RAISE(arrow::flight::FlightInfo info,
            arrow::flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, false));
        return std::make_unique<arrow::flight::FlightInfo>(info);
      }
    }

    ObFlightSqlQueryProcessor proc;
    const common::ObString sql_str(command.query.size(), command.query.c_str());
    int ret = proc.prepare(*session, sql_str);
    if (ret != OB_SUCCESS) {
      LOG_WARN("[FlightSQL] GetFlightInfoStatement: prepare failed",
               K(ret), "ob_trace_id", proc.trace_id().c_str(),
               "sql_id", proc.sql_id().c_str(), "plan_id", proc.plan_id());
      const std::string sql_preview =
          command.query.substr(0, std::min(size_t(200), command.query.size()));
      return ob_sql_exec_ret_to_arrow_status(
          ret, proc.trace_id().c_str(), sql_preview, &proc.error_detail());
    }

    const std::string query_id = next_query_id();
    {
      lib::ObMallocHookAttrGuard malloc_guard(
          common::ObMemAttr(session->tenant_id, "FlightSQLSess"),
          false /* use_500 */);
      std::lock_guard<std::mutex> guard(session->mutex);
      session->pending_sqls[query_id] = command.query;
    }

    const std::string handle = session_id + ":" + query_id;
    ARROW_ASSIGN_OR_RAISE(arrow::flight::Ticket ticket,
                          arrow::flight::sql::CreateStatementQueryTicket(handle));

    // Embed execution metadata in the FlightInfo schema — ADBC uses this schema
    // as the result schema (record_reader.go:86), so metadata here is visible to
    // the client via table.schema.metadata after fetchallarrow().
    std::shared_ptr<arrow::Schema> schema = proc.arrow_schema();
    {
      std::vector<std::string> keys, vals;
      if (!proc.trace_id().empty()) {
        keys.push_back("ob_trace_id");
        vals.push_back(proc.trace_id());
      }
      if (proc.plan_id() != 0) {
        keys.push_back("ob_plan_id");
        vals.push_back(std::to_string(proc.plan_id()));
      }
      if (!proc.sql_id().empty()) {
        keys.push_back("ob_sql_id");
        vals.push_back(proc.sql_id());
      }
      if (!keys.empty()) {
        schema = schema->WithMetadata(
            arrow::KeyValueMetadata::Make(std::move(keys), std::move(vals)));
      }
    }
    // emoty endpoints means DoGetStatement for the same server that issue GetFlightInfoStatement
    std::vector<arrow::flight::FlightEndpoint> endpoints{
        arrow::flight::FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};
    ARROW_ASSIGN_OR_RAISE(
        arrow::flight::FlightInfo info,
        arrow::flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, false));
    return std::make_unique<arrow::flight::FlightInfo>(info);
    } OB_FLIGHT_SQL_CATCH_AND_REMOVE_SESSION
  }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::StatementQueryTicket &command) override
  {
    try {
    LOG_TRACE("[FlightSQL] DoGetStatement",
             "peer", context.peer().c_str(),
             "handle", command.statement_handle.c_str());

    const std::string &handle = command.statement_handle;
    const size_t colon = handle.find(':');
    if (colon == std::string::npos) {
      return arrow::Status::Invalid("invalid ticket handle: ", handle);
    }
    const std::string session_id = handle.substr(0, colon);
    const std::string query_id   = handle.substr(colon + 1);

    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated,
                                            std::string("session not found: ") + session_id);
    }

    std::string sql;
    {
      lib::ObMallocHookAttrGuard malloc_guard(
          common::ObMemAttr(session->tenant_id, "FlightSQLSess"),
          false /* use_500 */);
      std::lock_guard<std::mutex> guard(session->mutex);
      std::unordered_map<std::string, std::string>::iterator it = session->pending_sqls.find(query_id);
      if (it == session->pending_sqls.end()) {
        return arrow::Status::Invalid("query_id not found: ", query_id);
      }
      sql = it->second;
      session->pending_sqls.erase(it);
    }

    // ── V2 ingest action ────────────────────────────────────────────────────
    if (sql.compare(0, strlen(OB_INGEST_ACTION_MARKER), OB_INGEST_ACTION_MARKER) == 0) {
      const std::string rest = sql.substr(strlen(OB_INGEST_ACTION_MARKER));
      const size_t colon2 = rest.find(':');
      if (colon2 == std::string::npos) {
        return arrow::Status::Invalid("[FlightSQL] malformed ingest action marker");
      }
      const std::string action      = rest.substr(0, colon2);
      const std::string body_json   = rest.substr(colon2 + 1);
      return dispatch_ingest_action(*session, action, body_json);
    }

    LOG_TRACE("[FlightSQL] DoGetStatement: executing",
             "query_id", query_id.c_str(),
             "sql_preview", sql.substr(0, std::min(size_t(100), sql.size())).c_str());

    if (context.is_cancelled()) {
      LOG_TRACE("[FlightSQL] DoGetStatement: RPC cancelled before execute",
               "session_id", session_id.c_str(),
               "query_id", query_id.c_str(),
               "peer", context.peer().c_str());
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Cancelled,
                                            "request cancelled before execution");
    }

    ObFlightSqlQueryProcessor proc;
    const common::ObString sql_str(sql.size(), sql.c_str());
    const int64_t request_time = common::ObTimeUtility::current_time();

    // Refresh local schema cache if behind the latest broadcast version.
    if (OB_NOT_NULL(GCTX.schema_service_)) {
      int64_t local_ver = 0, broadcast_ver = 0;
      if (OB_SUCCESS == GCTX.schema_service_->get_tenant_refreshed_schema_version(
              session->tenant_id, local_ver)
          && OB_SUCCESS == GCTX.schema_service_->get_tenant_received_broadcast_version(
              session->tenant_id, broadcast_ver)
          && local_ver < broadcast_ver) {
        (void)GCTX.schema_service_->async_refresh_schema(
            session->tenant_id, broadcast_ver);
      }
    }

    // ─── Lock session: same semantics as MySQL obmp_query.cpp LockGuard(query_lock).
    // Serializes queries on this session. trylock → reject concurrent DoGet with EAGAIN.
    // Lock ownership transfers to reader (unlock in ~ObFlightBatchReader).
    bool query_locked = false;
    if (OB_NOT_NULL(session->ob_session)) {
      if (OB_SUCCESS != session->ob_session->try_lock_query()) {
        return ob_sql_exec_ret_to_arrow_status(
            OB_EAGAIN, "", "concurrent query on same session rejected");
      }
      query_locked = true;
    }

    lib::MemoryContext mem_ctx;
    ObFlightSqlQueryPtr<common::ObArenaAllocator> arena_alloc;
    ObFlightSqlQueryPtr<share::schema::ObSchemaGetterGuard> schema_guard;
    ObFlightSqlQueryPtr<sql::ObSqlCtx> sql_ctx;
    ObFlightSqlQueryPtr<sql::ObResultSet> result_set;
    int exec_ret = proc.open_query(*session, sql_str, mem_ctx, arena_alloc, schema_guard, sql_ctx, result_set);
    const int64_t elapsed_time = common::ObTimeUtility::current_time() - request_time;

    if (exec_ret != OB_SUCCESS) {
      // open_query failed → unlock here (reader won't be constructed to do it).
      LOG_WARN_RET(exec_ret, "[FlightSQL] DoGetStatement: query open failed",
                   K(exec_ret), "ob_trace_id", proc.trace_id().c_str(),
                   "sql_id", proc.sql_id().c_str(), "plan_id", proc.plan_id(),
                   K(elapsed_time), "cancelled", context.is_cancelled());
      flight_sql_audit_failed_query(
          *session, proc, sql, request_time, exec_ret, "DoGetStatement");
      if (query_locked && OB_NOT_NULL(session->ob_session)) {
        (void)session->ob_session->unlock_query();
      }
      const std::string sql_preview = sql.substr(0, std::min(size_t(200), sql.size()));
      return ob_sql_exec_ret_to_arrow_status(
          exec_ret, proc.trace_id().c_str(), sql_preview, &proc.error_detail());
    }

    // After a successful open_query(), context.is_cancelled() is often true without a real
    // user cancel (client pipelining, deadlines, stream teardown). Do not return CANCELLED.
    if (context.is_cancelled()) {
      LOG_TRACE("[FlightSQL] DoGetStatement: is_cancelled after successful execute (ignored; "
                "often client/stream timing, not a real cancel)",
                "session_id", session_id.c_str(),
                "query_id", query_id.c_str(),
                "elapsed_us", elapsed_time,
                "peer", context.peer().c_str());
    }

    // Attach OceanBase execution metadata so clients can look up the plan.
    std::shared_ptr<arrow::Schema> arrow_schema = proc.arrow_schema();
    {
      std::vector<std::string> keys, vals;
      if (!proc.trace_id().empty()) {
        keys.push_back("ob_trace_id");
        vals.push_back(proc.trace_id());
      }
      if (proc.plan_id() != 0) {
        keys.push_back("ob_plan_id");
        vals.push_back(std::to_string(proc.plan_id()));
      }
      if (!proc.sql_id().empty()) {
        keys.push_back("ob_sql_id");
        vals.push_back(proc.sql_id());
      }
      if (!keys.empty()) {
        arrow_schema = arrow_schema->WithMetadata(
            arrow::KeyValueMetadata::Make(std::move(keys), std::move(vals)));
      }
    }

    // Build reader context. session (shared_ptr) keeps session alive for the streaming
    // duration; proc fields are copied out (proc is a DoGetStatement local).
    ObFlightReaderCtx reader_ctx;
    reader_ctx.session      = session;
    reader_ctx.sql_text     = sql;
    reader_ctx.plan_id      = proc.plan_id();
    reader_ctx.sql_id       = proc.sql_id();
    reader_ctx.trace_id     = proc.trace_id();
    reader_ctx.request_name = "DoGetStatement";
    reader_ctx.plan_type    = static_cast<int>(proc.plan_type());
    reader_ctx.stmt_type    = static_cast<int>(proc.stmt_type());
    reader_ctx.request_time = request_time;
    reader_ctx.resource_group_id = proc.resource_group_id();
    reader_ctx.timeout_ts   = proc.timeout_ts();
    std::shared_ptr<ObFlightBatchReader> reader = std::make_shared<ObFlightBatchReader>(
        arrow_schema, session->arrow_batch_size, session->tenant_id,
        mem_ctx, std::move(arena_alloc), std::move(schema_guard), std::move(sql_ctx),
        std::move(result_set), reader_ctx);
    if (query_locked) {
      reader->set_query_locked();  // transfer lock ownership to reader
    }
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    share::ObSystemTenantCgroupGuard cgroup_guard;
    int ret = OB_SUCCESS;
    if (OB_FAIL(tenant_guard.switch_to(session->tenant_id))) {
      return ob_sql_exec_ret_to_arrow_status(ret, proc.trace_id().c_str(), sql);
    } else if (OB_FAIL(cgroup_guard.switch_to(session->tenant_id,
                                             reader_ctx.resource_group_id))) {
      return ob_sql_exec_ret_to_arrow_status(ret, proc.trace_id().c_str(), sql);
    }
    // IPC compression (lz4 / zstd) negotiated via session option arrow.ipc.compression.
    arrow::ipc::IpcWriteOptions write_opts = arrow::ipc::IpcWriteOptions::Defaults();
    // Arrow's global CPU pool is outside this callback's cgroup scope.
    write_opts.use_threads = false;
    {
      int ret = OB_SUCCESS;  // required by LOG_WARN macro (LOG_ERRCODE_FOR_WARN expands to ret)
      const std::string &algo = session->compression_codec;
      arrow::Compression::type ct = arrow::Compression::UNCOMPRESSED;
      if (algo == "lz4") {
        ct = arrow::Compression::LZ4_FRAME;
      } else if (algo == "zstd") {
        ct = arrow::Compression::ZSTD;
      }
      if (ct != arrow::Compression::UNCOMPRESSED) {
        ArrowCodecResult codec_res = arrow::util::Codec::Create(ct);
        if (codec_res.ok()) {
          write_opts.codec = codec_res.MoveValueUnsafe();
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] DoGetStatement: failed to create compression codec",
                   K(ret), "algo", algo.c_str(),
                   "arrow_st", codec_res.status().ToString().c_str());
        }
      }
    }
    return std::make_unique<ObFlightSqlResultStream>(
        reader, write_opts, reader_ctx);

    } OB_FLIGHT_SQL_CATCH_AND_REMOVE_SESSION
  }

  // ============================================================
  // Session options: SetSessionOptions + GetSessionOptions
  // ============================================================

  arrow::Result<arrow::flight::SetSessionOptionsResult> SetSessionOptions(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::SetSessionOptionsRequest &request) override
  {
    try {
    int ret = OB_SUCCESS;
    {
      // Log every key in this call so we can diagnose multi-call ordering issues.
      std::string keys_str;
      for (const std::pair<const std::string, arrow::flight::SessionOptionValue> &kv :
           request.session_options) {
        if (!keys_str.empty()) { keys_str += ','; }
        keys_str += kv.first;
      }
      LOG_TRACE("[FlightSQL] SetSessionOptions",
               "peer", context.peer().c_str(),
               "option_count", static_cast<int>(request.session_options.size()),
               "keys", keys_str.c_str());
    }

    const std::string session_id = context.peer_identity();
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated,
                                            std::string("session not found: ") + session_id);
    }

    lib::ObMallocHookAttrGuard session_malloc_guard(
        common::ObMemAttr(session->tenant_id, "FlightSQLSess"),
        false /* use_500 */);

    SessionVarErrors var_errors;
    std::lock_guard<std::mutex> guard(session->mutex);

    // Pre-scan: collect catalog and schema in one pass, then apply catalog first, schema second,
    // so schema lookup always runs under the correct catalog context.
    const std::string *pre_catalog_key = nullptr;
    const std::string *pre_catalog_val = nullptr;
    const std::string *pre_schema_key  = nullptr;
    const std::string *pre_schema_val  = nullptr;
    for (const std::pair<const std::string, arrow::flight::SessionOptionValue> &kv :
         request.session_options) {
      if (kv.first == kSessionOptCatalog && std::holds_alternative<std::string>(kv.second)) {
        pre_catalog_key = &kv.first;
        pre_catalog_val = &std::get<std::string>(kv.second);
      } else if ((kv.first == kSessionOptSchema || kv.first == kSessionOptDbSchema)
                 && std::holds_alternative<std::string>(kv.second)
                 && pre_schema_val == nullptr) {
        pre_schema_key = &kv.first;
        pre_schema_val = &std::get<std::string>(kv.second);
      }
    }

    if (pre_catalog_val != nullptr) {
      const std::string &key = *pre_catalog_key;
      const std::string &cat = *pre_catalog_val;
      const int cat_ret = apply_catalog_to_session(*session, cat);
      mirror_flightsql_session_kv_to_sql_audit(
          *session, flightsql_audit_format_kv_(key, cat), cat_ret,
          "SetSessionOptions");
      if (OB_SUCCESS != cat_ret) {
        using arrow::flight::SetSessionOptionErrorValue;
        var_errors[key] = {(cat_ret == OB_ERR_BAD_DATABASE)
                               ? SetSessionOptionErrorValue::kInvalidValue
                               : SetSessionOptionErrorValue::kError};
      } else {
        LOG_TRACE("[FlightSQL] SetSessionOptions: catalog set", "catalog", cat.c_str());
        // No schema in this call: re-apply the existing schema under the new catalog.
        // If the schema does not exist in the new catalog, clear it silently.
        if (pre_schema_val == nullptr && !session->current_schema.empty()) {
          const int preserve_ret = apply_schema_to_session(*session, session->current_schema);
          if (OB_SUCCESS != preserve_ret) {
            LOG_TRACE("[FlightSQL] SetSessionOptions: schema not found in new catalog, cleared",
                     "schema", session->current_schema.c_str(), "catalog", cat.c_str());
            session->current_schema.clear();
          } else {
            LOG_TRACE("[FlightSQL] SetSessionOptions: schema preserved under new catalog",
                     "schema", session->current_schema.c_str(), "catalog", cat.c_str());
          }
        }
      }
    }

    if (pre_schema_val != nullptr) {
      const std::string &key = *pre_schema_key;
      const std::string &db  = *pre_schema_val;
      const int opt_ret = apply_schema_to_session(*session, db);
      if (OB_SUCCESS != opt_ret) {
        LOG_WARN("[FlightSQL] SetSessionOptions: set_default_database failed",
                 K(opt_ret), "db", db.c_str(), "current_catalog", session->current_catalog.c_str());
        var_errors[key] = {arrow::flight::SetSessionOptionErrorValue::kError};
      } else {
        LOG_TRACE("[FlightSQL] SetSessionOptions: schema set",
                 "schema", db.c_str(), "catalog", session->current_catalog.c_str());
      }
      mirror_flightsql_session_kv_to_sql_audit(
          *session, flightsql_audit_format_kv_(key, db), opt_ret,
          "SetSessionOptions");
    }

    // Main loop: all other options (catalog and schema already handled above).
    for (const std::pair<const std::string, arrow::flight::SessionOptionValue> &kv :
         request.session_options) {
      const std::string &key = kv.first;
      const arrow::flight::SessionOptionValue &val = kv.second;
      ret = OB_SUCCESS;

      if (key == kSessionOptCatalog || key == kSessionOptSchema || key == kSessionOptDbSchema) {
        // already applied above in catalog-before-schema order
      } else if (key == kSessionOptObSessionVars) {
        if (std::holds_alternative<std::string>(val)) {
          const std::string &encoded = std::get<std::string>(val);
          apply_session_vars(
              *session, encoded, var_errors, "SetSessionOptions");
        }
      } else if (key == kSessionOptBatchSize) {
        int64_t bs = 0;
        if (std::holds_alternative<int64_t>(val)) {
          bs = std::get<int64_t>(val);
        } else if (std::holds_alternative<std::string>(val)) {
          try { bs = std::stoll(std::get<std::string>(val)); } catch (...) { bs = 0; }
        }
        if (bs >= 1 && bs <= 65536) {
          session->arrow_batch_size = bs;
          LOG_TRACE("[FlightSQL] SetSessionOptions: batch_size set",
                   "batch_size", static_cast<int64_t>(bs));
          mirror_flightsql_session_kv_to_sql_audit(
              *session,
              flightsql_audit_format_kv_(key, std::to_string(bs)),
              OB_SUCCESS, "SetSessionOptions");
        } else {
          LOG_WARN("[FlightSQL] SetSessionOptions: ob_flight_sql_batch_size out of range [1,65536]",
                   "value", static_cast<int64_t>(bs));
          mirror_flightsql_session_kv_to_sql_audit(
              *session,
              flightsql_audit_format_kv_(key, std::to_string(bs)),
              OB_ERR_UNEXPECTED, "SetSessionOptions");
        }
      } else if (key == kSessionOptCompression) {
        if (std::holds_alternative<std::string>(val)) {
          const std::string &algo = std::get<std::string>(val);
          if (algo == "none" || algo == "lz4" || algo == "zstd") {
            session->compression_codec = algo;
          } else {
            // Unknown algo: silently fall back to no compression.
            session->compression_codec = "none";
            LOG_TRACE("[FlightSQL] SetSessionOptions: unknown compression algo, falling back to none",
                      "algo", algo.c_str());
          }
          LOG_TRACE("[FlightSQL] SetSessionOptions: compression set",
                    "requested", algo.c_str(), "effective", session->compression_codec.c_str());
          mirror_flightsql_session_kv_to_sql_audit(*session,
              flightsql_audit_format_kv_(key, session->compression_codec), OB_SUCCESS,
              "SetSessionOptions");
        }
      } else {
        LOG_TRACE("[FlightSQL] SetSessionOptions: ignoring unknown key", "key", key.c_str());
      }
    }
    arrow::flight::SetSessionOptionsResult result;
    result.errors = std::move(var_errors);
    return result;
    } OB_FLIGHT_SQL_CATCH_ALL
  }

  arrow::Result<arrow::flight::GetSessionOptionsResult> GetSessionOptions(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::GetSessionOptionsRequest &request) override
  {
    try {
    UNUSED(request);
    LOG_TRACE("[FlightSQL] GetSessionOptions", "peer", context.peer().c_str());

    const std::string session_id = context.peer_identity();
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated,
                                            std::string("session not found: ") + session_id);
    }

    arrow::flight::GetSessionOptionsResult result;
    std::lock_guard<std::mutex> guard(session->mutex);
    if (!session->current_schema.empty()) {
      result.session_options[kSessionOptSchema] =
          arrow::flight::SessionOptionValue{session->current_schema};
    }
    if (!session->current_catalog.empty()) {
      result.session_options[kSessionOptCatalog] =
          arrow::flight::SessionOptionValue{session->current_catalog};
    }
    result.session_options[kSessionOptCompression] =
        arrow::flight::SessionOptionValue{session->compression_codec};
    result.session_options[kSessionOptBatchSize] =
        arrow::flight::SessionOptionValue{session->arrow_batch_size};
    return result;
    } OB_FLIGHT_SQL_CATCH_ALL
  }

  // ============================================================
  // CloseSession
  // ============================================================

  arrow::Result<arrow::flight::CloseSessionResult> CloseSession(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::CloseSessionRequest &request) override
  {
    try {
    UNUSED(request);
    const std::string session_id = context.peer_identity();
    LOG_TRACE("[FlightSQL] CloseSession", "peer", context.peer().c_str(),
             "session_id", session_id.c_str());
    if (auto session = session_mgr_.get(session_id)) {
      std::shared_ptr<ObFlightSqlRequestCancellation> cancellation;
      {
        std::lock_guard<std::mutex> guard(session->mutex);
        session->closing_ = true;
        cancellation = session->active_doput_cancellation;
      }
      if (cancellation) { (void)cancellation->cancel([] {}); }
    }
    session_mgr_.remove(session_id);
    return arrow::flight::CloseSessionResult{arrow::flight::CloseSessionStatus::kClosed};
    } OB_FLIGHT_SQL_CATCH_ALL
  }

  // ============================================================
  // Standard DDL/DML: DoPutCommandStatementUpdate
  // ============================================================

  arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::StatementUpdate &command) override
  {
    try {
    ObFlightWorkerGuard flight_worker_guard;
    int ret = OB_SUCCESS;
    common::ObCurTraceId::init(GCTX.self_addr());
    const std::string session_id = context.peer_identity();
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          std::string("session not found: ") + session_id);
    }
    if (command.query.empty()) {
      return arrow::Status::Invalid("[FlightSQL] StatementUpdate query is empty");
    }

    ObSessionDoPutGuard doput_guard(*session);
    if (!doput_guard.acquired()) {
      return arrow::Status::Invalid(
          "[FlightSQL] concurrent DoPut on the same session is not supported or session is closing");
    }
    ObSessionQueryGuard query_guard(*session);
    if (!query_guard.acquired()) {
      return arrow::Status::Invalid("concurrent queries on the same session are not supported");
    }
    ObFlightSqlDoPutCancellationScope cancellation(*session, context, rpc_cancel_registry_);
    if (!cancellation.request()) {
      return arrow::Status::IOError("[FlightSQL] DoPut cancellation context unavailable");
    }
    cancellation.activate();
    if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
    const common::ObString sql_str(command.query.size(), command.query.c_str());
    ObFlightSqlQueryProcessor proc;
    if (OB_FAIL(proc.prepare(
            *session, sql_str,
            ObFlightSqlQueryProcessor::PrepareMode::STATEMENT_UPDATE, true))) {
      if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
      const std::string sql_preview = command.query.substr(
          0, std::min(size_t(200), command.query.size()));
      return ob_sql_exec_ret_to_arrow_status(ret, proc.trace_id().c_str(), sql_preview);
    } else if (OB_FAIL(proc.execute(
                   *session, sql_str, "DoPutCommandStatementUpdate", true, cancellation.request().get()))) {
      if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
      const std::string sql_preview = command.query.substr(
          0, std::min(size_t(200), command.query.size()));
      return ob_sql_exec_ret_to_arrow_status(ret, proc.trace_id().c_str(), sql_preview);
    }

    const int64_t affected_rows = proc.affected_rows();
    LOG_TRACE("[FlightSQL] DoPutCommandStatementUpdate: OK",
              "session_id", session_id.c_str(), "affected_rows", affected_rows,
              "trace_id", proc.trace_id().c_str());
    if (cancellation.finish()) { return flight_sql_doput_cancelled(); }
    return affected_rows;
    } OB_FLIGHT_SQL_CATCH_AND_REMOVE_SESSION
  }

  // ============================================================
  // Prepared DML
  // ============================================================

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoPreparedStatement(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::PreparedStatementQuery &,
      const arrow::flight::FlightDescriptor &) override
  { return arrow::Status::NotImplemented("PreparedStatement not implemented"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetPreparedStatement(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::PreparedStatementQuery &) override
  { return arrow::Status::NotImplemented("PreparedStatement not implemented"); }

  arrow::Status DoPutPreparedStatementQuery(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::PreparedStatementQuery &,
      arrow::flight::FlightMessageReader *,
      arrow::flight::FlightMetadataWriter *) override
  { return arrow::Status::NotImplemented("PreparedStatement not implemented"); }

  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::PreparedStatementUpdate &command,
      arrow::flight::FlightMessageReader *reader) override
  {
    try {
    ObFlightWorkerGuard flight_worker_guard;
    int ret = OB_SUCCESS;
    common::ObCurTraceId::init(GCTX.self_addr());
    const std::string session_id = context.peer_identity();
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          std::string("session not found: ") + session_id);
    } else if (OB_ISNULL(reader)) {
      return arrow::Status::Invalid("[FlightSQL] prepared update reader is null");
    }

    ObSessionDoPutGuard doput_guard(*session);
    if (!doput_guard.acquired()) {
      return arrow::Status::Invalid(
          "[FlightSQL] concurrent DoPut on the same session is not supported; "
          "use a separate Flight SQL session");
    }
    std::unique_lock<std::mutex> prepared_lock(session->prepared_mutex);
    ObSessionQueryGuard query_guard(*session);
    if (!query_guard.acquired()) {
      return arrow::Status::Invalid(
          "concurrent queries on the same session are not supported");
    }
    ObFlightSqlDoPutCancellationScope cancellation(*session, context, rpc_cancel_registry_);
    if (!cancellation.request()) {
      return arrow::Status::IOError("[FlightSQL] DoPut cancellation context unavailable");
    }
    cancellation.activate();
    if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
    if (!session->prepared.active) {
      return arrow::Status::Invalid("[FlightSQL] no active prepared statement");
    } else if (command.prepared_statement_handle != session->prepared.opaque_handle) {
      return arrow::Status::Invalid("[FlightSQL] stale prepared statement handle");
    }

    const int64_t request_timeout_ts =
        ObFlightSqlQueryProcessor::start_request_timeout(*session);
    lib::WorkerTimeoutGuard request_timeout_guard(request_timeout_ts);
    auto schema_result = reader->GetSchema();
    if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> stream_schema,
                          std::move(schema_result));
    if (!stream_schema) {
      return arrow::Status::Invalid("[FlightSQL] prepared parameter schema is null");
    } else if (stream_schema->num_fields() != session->prepared.parameter_count) {
      std::ostringstream msg;
      msg << "[FlightSQL] prepared parameter count mismatch: expected "
          << session->prepared.parameter_count << ", got "
          << stream_schema->num_fields();
      return arrow::Status::Invalid(msg.str());
    } else if (OB_FAIL(ObFlightSqlParamConverter::validate_schema(
                   *stream_schema, *session))) {
      const std::string detail_msg =
          flight_sql_prepared_parameter_error_message(ret, stream_schema);
      return ob_sql_exec_ret_to_arrow_status(
          ret, nullptr, "prepared parameter schema", &detail_msg);
    }

    common::ObArenaAllocator row_allocator(
        "FlightSQLPSPar", OB_MALLOC_NORMAL_BLOCK_SIZE,
        static_cast<int64_t>(session->tenant_id));
    ObFlightSqlQueryProcessor proc;
    int64_t total_affected_rows = 0;
    int64_t parameter_row_idx = 0;
    while (true) {
      if (cancellation.is_cancelled()) {
        return arrow::flight::MakeFlightError(
            arrow::flight::FlightStatusCode::Cancelled,
            "[FlightSQL] prepared update was cancelled");
      }
      auto chunk_result = reader->Next();
      if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
      ARROW_ASSIGN_OR_RAISE(arrow::flight::FlightStreamChunk chunk, std::move(chunk_result));
      if (!chunk.data && !chunk.app_metadata) {
        break;
      } else if (!chunk.data) {
        continue;
      }
      const std::shared_ptr<arrow::RecordBatch> &batch = chunk.data;
      if (batch->num_columns() != session->prepared.parameter_count) {
        std::ostringstream msg;
        msg << "[FlightSQL] prepared parameter count mismatch in RecordBatch: expected "
            << session->prepared.parameter_count << ", got " << batch->num_columns();
        return arrow::Status::Invalid(msg.str());
      }

      for (int64_t batch_row_idx = 0; batch_row_idx < batch->num_rows(); ++batch_row_idx) {
        if (cancellation.is_cancelled()) {
          return arrow::flight::MakeFlightError(
              arrow::flight::FlightStatusCode::Cancelled,
              "[FlightSQL] prepared update was cancelled before parameter row "
                  + std::to_string(parameter_row_idx));
        }

        int64_t row_affected_rows = 0;
        {
          int64_t failed_column = -1;
          const common::ObString sql_text(
              static_cast<int32_t>(session->prepared.sql_text.size()),
              session->prepared.sql_text.c_str());
          if (OB_FAIL(proc.execute_prepared_update(
                  *session, session->prepared.native_client_stmt_id,
                  session->prepared.statement_type, *batch, batch_row_idx,
                  row_allocator, sql_text, session->prepared.sensitive_sql,
                  "DoPutPreparedStatementUpdate", failed_column, cancellation.request().get()))) {
            if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
            std::string preview = "prepared parameter row "
                + std::to_string(parameter_row_idx) + "; sql: ";
            if (session->prepared.sensitive_sql) {
              preview.append(OB_MASKED_STR);
            } else {
              preview.append(session->prepared.sql_text.substr(
                  0, std::min(size_t(200), session->prepared.sql_text.size())));
            }
            if (failed_column >= 0) {
              std::string detail_msg =
                  flight_sql_prepared_parameter_error_message(ret, stream_schema);
              if (detail_msg.empty()) {
                std::ostringstream msg;
                msg << "[FlightSQL] failed to bind prepared parameter row "
                    << parameter_row_idx << ", column " << failed_column
                    << ": " << ob_strerror(ret) << " (" << ob_error_name(ret) << ")";
                detail_msg = msg.str();
              } else {
                detail_msg.append(" at prepared parameter row ");
                detail_msg.append(std::to_string(parameter_row_idx));
                detail_msg.append(", column ");
                detail_msg.append(std::to_string(failed_column));
              }
              return ob_sql_exec_ret_to_arrow_status(
                  ret, proc.trace_id().c_str(), preview, &detail_msg);
            }
            return ob_sql_exec_ret_to_arrow_status(
                ret, proc.trace_id().c_str(), preview);
          }
          row_affected_rows = proc.affected_rows();
        }
        row_allocator.reset();

        if (row_affected_rows < 0
            || row_affected_rows > std::numeric_limits<int64_t>::max()
                                      - total_affected_rows) {
          return arrow::Status::Invalid(
              "[FlightSQL] prepared update affected-row total overflow at parameter row "
              + std::to_string(parameter_row_idx));
        }
        total_affected_rows += row_affected_rows;
        if (parameter_row_idx == std::numeric_limits<int64_t>::max()) {
          return arrow::Status::Invalid(
              "[FlightSQL] prepared parameter row index overflow");
        }
        ++parameter_row_idx;
      }
    }

    LOG_TRACE("[FlightSQL] DoPutPreparedStatementUpdate: OK",
              "session_id", session_id.c_str(),
              "parameter_rows", parameter_row_idx,
              "affected_rows", total_affected_rows,
              "trace_id", common::ObCurTraceId::get_trace_id_str());
    if (cancellation.finish()) { return flight_sql_doput_cancelled(); }
    return total_affected_rows;
    } OB_FLIGHT_SQL_CATCH_AND_REMOVE_SESSION
  }

  arrow::Result<arrow::flight::sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::ActionCreatePreparedStatementRequest &request) override
  {
    try {
    int ret = OB_SUCCESS;
    common::ObCurTraceId::init(GCTX.self_addr());
    const std::string session_id = context.peer_identity();
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          std::string("session not found: ") + session_id);
    } else if (!request.transaction_id.empty()) {
      return arrow::Status::NotImplemented(
          "[FlightSQL] prepared statements inside protocol transactions are not supported");
    } else if (!request.query.empty() && request.query[0] == '{'
               && !ob_json_str(request.query, "ob_action").empty()) {
      // Ingest control actions are handled by GetFlightInfoStatement and
      // DoGetStatement.  They are not SQL and must not reach stmt_prepare().
      return arrow::Status::NotImplemented(
          "[FlightSQL] ingest control actions are not prepared statements");
    }

    std::unique_lock<std::mutex> prepared_lock(session->prepared_mutex);
    ObSessionQueryGuard query_guard(*session);
    if (!query_guard.acquired()) {
      return arrow::Status::Invalid(
          "concurrent queries on the same session are not supported");
    }
    const int64_t request_timeout_ts =
        ObFlightSqlQueryProcessor::start_request_timeout(*session);
    lib::WorkerTimeoutGuard request_timeout_guard(request_timeout_ts);

    const bool had_active_statement = session->prepared.active;
    const common::ObPsStmtId old_client_stmt_id =
        session->prepared.native_client_stmt_id;
    {
      lib::ObMallocHookAttrGuard malloc_guard(
          common::ObMemAttr(session->tenant_id, "FlightSQLSess"), false /* use_500 */);
      session->prepared.reset();
    }
    if (had_active_statement) {
      if (common::OB_INVALID_ID == old_client_stmt_id) {
        ret = OB_ERR_UNEXPECTED;
        return ob_sql_exec_ret_to_arrow_status(
            ret, common::ObCurTraceId::get_trace_id_str(),
            "replace prepared statement");
      } else if (OB_FAIL(ObFlightSqlQueryProcessor::close_prepared_statement(
                     *session, old_client_stmt_id))) {
        return ob_sql_exec_ret_to_arrow_status(
            ret, common::ObCurTraceId::get_trace_id_str(),
            "replace prepared statement");
      }
    }

    if (session->prepared_generation_ == std::numeric_limits<uint64_t>::max()) {
      return arrow::Status::CapacityError(
          "[FlightSQL] prepared statement generation exhausted");
    }
    const uint64_t generation = ++session->prepared_generation_;
    if (request.query.empty()) {
      return arrow::Status::Invalid("[FlightSQL] prepared statement query is empty");
    }

    const common::ObString sql_text(
        static_cast<int32_t>(request.query.size()), request.query.c_str());
    ObFlightSqlQueryProcessor proc;
    common::ObPsStmtId client_stmt_id = common::OB_INVALID_ID;
    sql::stmt::StmtType stmt_type = sql::stmt::T_NONE;
    int64_t parameter_count = 0;
    bool sensitive_sql = false;
    if (OB_FAIL(proc.prepare_prepared_update(
            *session, sql_text, client_stmt_id, stmt_type,
            parameter_count, sensitive_sql, "CreatePreparedStatement"))) {
      if (common::OB_INVALID_ID != client_stmt_id) {
        const int close_ret = ObFlightSqlQueryProcessor::close_prepared_statement(
            *session, client_stmt_id);
        if (OB_SUCCESS != close_ret) {
          LOG_WARN("[FlightSQL] failed to close rejected prepared statement",
                   K(close_ret), K(client_stmt_id));
        }
      }
      const std::string preview = sensitive_sql
          ? std::string(OB_MASKED_STR)
          : request.query.substr(0, std::min(size_t(200), request.query.size()));
      return ob_sql_exec_ret_to_arrow_status(
          ret, proc.trace_id().c_str(), preview);
    }

    arrow::flight::sql::ActionCreatePreparedStatementResult result;
    result.dataset_schema = arrow::schema({});
    result.parameter_schema = proc.arrow_schema()
        ? proc.arrow_schema() : arrow::schema({});
    result.prepared_statement_handle =
        flight_sql_make_prepared_handle(generation);

    {
      lib::ObMallocHookAttrGuard malloc_guard(
          common::ObMemAttr(session->tenant_id, "FlightSQLSess"), false /* use_500 */);
      session->prepared.active = true;
      session->prepared.generation = generation;
      session->prepared.opaque_handle = result.prepared_statement_handle;
      session->prepared.native_client_stmt_id = client_stmt_id;
      session->prepared.statement_type = stmt_type;
      session->prepared.parameter_count = parameter_count;
      session->prepared.sql_text = request.query;
      session->prepared.sensitive_sql = sensitive_sql;
    }

    LOG_TRACE("[FlightSQL] CreatePreparedStatement: OK",
              "session_id", session_id.c_str(),
              "generation", generation,
              "native_client_stmt_id", client_stmt_id,
              "stmt_type", stmt_type,
              "parameter_count", parameter_count,
              "trace_id", proc.trace_id().c_str());
    return result;
    } OB_FLIGHT_SQL_CATCH_AND_REMOVE_SESSION
  }

  arrow::Status ClosePreparedStatement(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::ActionClosePreparedStatementRequest &request) override
  {
    try {
    int ret = OB_SUCCESS;
    common::ObCurTraceId::init(GCTX.self_addr());
    const std::string session_id = context.peer_identity();
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          std::string("session not found: ") + session_id);
    }

    std::unique_lock<std::mutex> prepared_lock(session->prepared_mutex);
    ObSessionQueryGuard query_guard(*session);
    if (!query_guard.acquired()) {
      return arrow::Status::Invalid(
          "concurrent queries on the same session are not supported");
    }
    if (!session->prepared.active
        || request.prepared_statement_handle != session->prepared.opaque_handle) {
      return arrow::Status::OK();
    }

    const int64_t request_timeout_ts =
        ObFlightSqlQueryProcessor::start_request_timeout(*session);
    lib::WorkerTimeoutGuard request_timeout_guard(request_timeout_ts);
    const common::ObPsStmtId client_stmt_id =
        session->prepared.native_client_stmt_id;
    {
      lib::ObMallocHookAttrGuard malloc_guard(
          common::ObMemAttr(session->tenant_id, "FlightSQLSess"), false /* use_500 */);
      session->prepared.reset();
    }
    if (OB_FAIL(ObFlightSqlQueryProcessor::close_prepared_statement(
            *session, client_stmt_id))) {
      return ob_sql_exec_ret_to_arrow_status(
          ret, common::ObCurTraceId::get_trace_id_str(),
          "close prepared statement");
    }
    LOG_TRACE("[FlightSQL] ClosePreparedStatement: OK",
              "session_id", session_id.c_str(),
              "native_client_stmt_id", client_stmt_id);
    return arrow::Status::OK();
    } OB_FLIGHT_SQL_CATCH_AND_REMOVE_SESSION
  }

  // ============================================================
  // Bulk ingest: DoPutCommandStatementIngest
  // ============================================================

  arrow::Result<int64_t> DoPutCommandStatementIngest(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::StatementIngest &command,
      arrow::flight::FlightMessageReader *reader) override
  {
    try {
    ObFlightWorkerGuard flight_worker_guard;
    int ret = OB_SUCCESS;
    common::ObCurTraceId::init(GCTX.self_addr());
    LOG_TRACE("[FlightSQL] DoPutCommandStatementIngest: start",
              "peer", context.peer().c_str(),
              "table", command.table.c_str(),
              "schema", command.schema.value_or("(session default)").c_str(),
              "catalog", command.catalog.value_or("(session default)").c_str());

    // ── 1. Session ──────────────────────────────────────────────
    const std::string session_id = context.peer_identity();
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          std::string("session not found: ") + session_id);
    }
    ObSessionDoPutGuard doput_guard(*session);
    if (!doput_guard.acquired()) {
      return arrow::Status::Invalid(
          "[FlightSQL] concurrent DoPut on the same session is not supported; "
          "use a separate Flight SQL session");
    }

    const auto ingest_header = context.incoming_headers().find(FLIGHT_SQL_HEADER_INGEST_ID);
    const bool multistream = ingest_header != context.incoming_headers().end()
        && !ingest_header->second.empty();
    std::optional<ObSessionQueryGuard> query_guard;
    if (!multistream) {
      query_guard.emplace(*session);
      if (!query_guard->acquired()) {
        return arrow::Status::Invalid("concurrent queries on the same session are not supported");
      }
    }
    ObFlightSqlDoPutCancellationScope cancellation(
        *session, context, rpc_cancel_registry_, !multistream);
    if (!cancellation.request()) {
      return arrow::Status::IOError("[FlightSQL] DoPut cancellation context unavailable");
    }
    cancellation.activate();
    if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }

    // ── 2. Arrow schema from the incoming stream ────────────────
    auto schema_result = reader->GetSchema();
    if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> arrow_schema,
                          std::move(schema_result));
    if (!arrow_schema || arrow_schema->num_fields() == 0) {
      return arrow::Status::Invalid(
          "[FlightSQL] Arrow schema is empty — no columns to ingest");
    }
    {
      using TblExists = arrow::flight::sql::TableDefinitionOptionsTableExistsOption;
      if (command.table_definition_options.if_exists != TblExists::kAppend) {
        return arrow::Status::Invalid(
            "[FlightSQL] only TableDefinitionOptions.if_exists=APPEND is supported; "
            "use ob_write_mode=upsert for row-level upsert");
      }
    }

    // ── 3. Resolve target table ─────────────────────────────────
    // SetSessionOptions updates these strings under session->mutex.  Snapshot
    // both before passing them to value_or(), whose default argument is
    // evaluated even when the command supplies catalog/schema explicitly.
    std::string session_catalog;
    std::string session_schema;
    {
      std::lock_guard<std::mutex> guard(session->mutex);
      session_catalog = session->current_catalog;
      session_schema = session->current_schema;
    }
    const std::string catalog_name = command.catalog.value_or(session_catalog);
    const std::string db_name = command.schema.value_or(session_schema);
    const std::string &table_name = command.table;
    const bool is_oracle_mode = OB_NOT_NULL(session->ob_session)
        && session->ob_session->is_oracle_compatible();
    if (db_name.empty()) {
      return arrow::Status::Invalid(
          "[FlightSQL] No database specified: set 'schema' in StatementIngest "
          "or use SetSessionOptions(\"schema\", \"your_db\")");
    }
    if (table_name.empty()) {
      return arrow::Status::Invalid("[FlightSQL] Table name is empty");
    }
    if (!flight_sql_is_safe_identifier(db_name)
        || !flight_sql_is_safe_identifier(table_name)) {
      return arrow::Status::Invalid(
          "[FlightSQL] database and table names must not contain SQL quote or control characters");
    }
    if (is_oracle_mode
        && (!flight_sql_is_safe_oracle_identifier(db_name)
            || !flight_sql_is_safe_oracle_identifier(table_name))) {
      return arrow::Status::Invalid(
          "[FlightSQL] Oracle-mode ingest requires regular unquoted database and table identifiers");
    }

    // External catalogs not supported in V1.
    if (!catalog_name.empty()
        && 0 != strcasecmp(catalog_name.c_str(), OB_INTERNAL_CATALOG_NAME)) {
      return arrow::Status::NotImplemented(
          "[FlightSQL] ingest into external catalog '" + catalog_name +
          "' is not supported yet");
    }

    // ── Multi-stream ingest: detect ob_ingest_id header ─────────────────────────
    std::string ingest_id_hdr;
    std::string ingest_arrow_schema_hdr;
    int64_t     streams_per_node = 1;
    {
      const arrow::flight::CallHeaders &hdrs = context.incoming_headers();
      arrow::flight::CallHeaders::const_iterator it_id =
          hdrs.find(FLIGHT_SQL_HEADER_INGEST_ID);
      if (it_id != hdrs.end() && !it_id->second.empty()) {
        ingest_id_hdr = std::string(it_id->second);
        arrow::flight::CallHeaders::const_iterator it_streams =
            hdrs.find(FLIGHT_SQL_HEADER_INGEST_STREAMS_PER_NODE);
        if (it_streams != hdrs.end() && !it_streams->second.empty()) {
          try { streams_per_node = std::stoll(std::string(it_streams->second)); }
          catch (...) { streams_per_node = 1; }
        }
        arrow::flight::CallHeaders::const_iterator it_schema =
            hdrs.find(FLIGHT_SQL_HEADER_INGEST_ARROW_SCHEMA);
        if (it_schema != hdrs.end() && !it_schema->second.empty()) {
          ingest_arrow_schema_hdr = std::string(it_schema->second);
        }
      }
    }
    if (!ingest_id_hdr.empty()) {
      // These modes are V1 single-stream extensions. Do not silently ignore
      // their options while registering a V2/V3 worker slot.
      const auto mode_it = command.options.find(FLIGHT_SQL_HEADER_WRITE_MODE);
      common::ObString slot_mode;
      if (mode_it != command.options.end()) {
        slot_mode.assign_ptr(mode_it->second.data(), mode_it->second.size());
      }
      const auto &hdrs = context.incoming_headers();
      const auto header_it = hdrs.find(FLIGHT_SQL_HEADER_WRITE_MODE);
      if (header_it != hdrs.end()) {
        slot_mode.assign_ptr(header_it->second.data(), header_it->second.size());
      }
      const auto merge_it = command.options.find(FLIGHT_SQL_HEADER_MERGE_OPTIONS);
      common::ObString slot_merge;
      if (merge_it != command.options.end()) {
        slot_merge.assign_ptr(merge_it->second.data(), merge_it->second.size());
      }
      const auto merge_header_it = hdrs.find(FLIGHT_SQL_HEADER_MERGE_OPTIONS);
      if (merge_header_it != hdrs.end()) {
        slot_merge.assign_ptr(merge_header_it->second.data(), merge_header_it->second.size());
      }
      if (slot_mode == FLIGHT_SQL_WRITE_MODE_MERGE || !slot_merge.empty()) {
        return arrow::Status::NotImplemented("[FlightSQL] merge ingest supports V1 single-stream only");
      }
      if (slot_mode == FLIGHT_SQL_WRITE_MODE_OVERWRITE
          || command.options.find(FLIGHT_SQL_HEADER_OVERWRITE_OPTIONS) != command.options.end()
          || hdrs.find(FLIGHT_SQL_HEADER_OVERWRITE_OPTIONS) != hdrs.end()) {
        return arrow::Status::NotImplemented("[FlightSQL] overwrite ingest supports V1 single-stream only");
      }
    }
    std::shared_ptr<arrow::Schema> advertised_ingest_schema;
    if (!ingest_id_hdr.empty()) {
      ARROW_ASSIGN_OR_RAISE(advertised_ingest_schema,
                            ob_decode_ingest_arrow_schema(ingest_arrow_schema_hdr));
      if (!advertised_ingest_schema->Equals(*arrow_schema, false)) {
        return arrow::Status::Invalid(
            "[FlightSQL] DoPut Arrow schema differs from the advertised ingest schema");
      }
    }

    // ── 4. Build column mapping from Arrow schema (no OB schema access here) ──
    ObFlightSqlColumnMapping column_mapping;
    column_mapping.set_attr(
        common::ObMemAttr(session->tenant_id, "FlightSQLMap"));
    std::vector<std::string> col_names;

    for (int i = 0; i < arrow_schema->num_fields(); ++i) {
      ObFlightSqlColumnDesc desc;
      desc.arrow_field_idx   = i;
      desc.ob_schema         = nullptr;
      desc.kind              = ConversionKind::EXACT;
      desc.ts_unit_factor    = 0;
      desc.decimal_int_bytes = 0;
      desc.convert_batch     = nullptr;
      desc.convert_datum     = nullptr;
      if (OB_FAIL(column_mapping.push_back(desc))) {
        return arrow::Status::Invalid(
            "[FlightSQL] failed to build column mapping");
      }
      const std::string &col_name = arrow_schema->field(i)->name();
      if (!flight_sql_is_safe_identifier(col_name)) {
        return arrow::Status::Invalid(
            "[FlightSQL] Arrow field names must not contain SQL quote or control characters");
      }
      if (is_oracle_mode && !flight_sql_is_safe_oracle_identifier(col_name)) {
        return arrow::Status::Invalid(
            "[FlightSQL] Oracle-mode ingest requires regular unquoted Arrow field names");
      }
      col_names.push_back(col_name);
    }

    // ── 5. Register stream ──────────────────────────────────────
    const uint64_t stream_id =
        session->next_stream_id_.fetch_add(1, std::memory_order_relaxed);

    ret = session->stream_registry.register_schema(
        session->tenant_id, stream_id, arrow_schema, column_mapping,
        db_name, table_name);
    if (OB_SUCCESS != ret) {
      return arrow::Status::Invalid(
          "[FlightSQL] failed to register arrow stream entry");
    }

    // FlightMessageReader extends MetadataRecordBatchReader (not RecordBatchReader).
    // Wrap it so the registry and ObFunctionTableOp can use RecordBatchReader::ReadNext.
    struct FlightReaderAdapter final : public arrow::RecordBatchReader {
      arrow::flight::FlightMessageReader *r_;
      const arrow::flight::ServerCallContext &context_;
      FlightReaderAdapter(arrow::flight::FlightMessageReader *r,
                          const arrow::flight::ServerCallContext &context)
          : r_(r), context_(context) {}
      std::shared_ptr<arrow::Schema> schema() const override {
        arrow::Result<std::shared_ptr<arrow::Schema>> res = r_->GetSchema();
        return res.ok() ? res.MoveValueUnsafe() : nullptr;
      }
      arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override {
        int ret = OB_SUCCESS;
        if (OB_ISNULL(r_)) {
          return arrow::Status::Invalid("[FlightSQL] FlightReaderAdapter: reader is null");
        }
        try {
          while (true) {
            arrow::Result<arrow::flight::FlightStreamChunk> chunk_result = r_->Next();
            if (!chunk_result.ok()) {
              ret = OB_IO_ERROR;
              LOG_WARN("[FlightSQL] Flight reader Next failed",
                       "arrow_error", chunk_result.status().ToString().c_str());
              return chunk_result.status();
            }
            arrow::flight::FlightStreamChunk chunk =
                std::move(chunk_result).MoveValueUnsafe();
            if (context_.is_cancelled()) {
              LOG_TRACE("[FlightSQL] Flight reader cancelled while reading DoPut stream",
                        "has_data", !OB_ISNULL(chunk.data),
                        "has_app_metadata", !OB_ISNULL(chunk.app_metadata));
              return arrow::Status::Cancelled(
                  "[FlightSQL] client cancelled DoPut while reading Arrow stream");
            } else if (OB_NOT_NULL(chunk.data)) {
              *batch = std::move(chunk.data);
              return arrow::Status::OK();
            } else if (OB_ISNULL(chunk.app_metadata)) {
              batch->reset();
              return arrow::Status::OK();
            } else {
              LOG_DEBUG("[FlightSQL] skipping metadata-only DoPut message");
            }
          }
        } catch (const std::exception &ex) {
          return arrow::Status::IOError("[FlightSQL] ReadNext exception: ", ex.what());
        } catch (...) {
          return arrow::Status::IOError("[FlightSQL] ReadNext unknown exception");
        }
      }
    };
    FlightReaderAdapter reader_adapter(reader, context);
    ret = session->stream_registry.attach_reader(stream_id, &reader_adapter);
    if (OB_SUCCESS != ret) {
      const int unregister_ret = session->stream_registry.unregister(stream_id);
      if (OB_SUCCESS != unregister_ret) {
        LOG_WARN("[FlightSQL] failed to clean up stream after attach failure",
                 K(unregister_ret), K(stream_id));
      }
      return arrow::Status::Invalid(
          "[FlightSQL] failed to attach reader to stream entry");
    }

    ret = session_mgr_.register_active_stream_session(session);
    if (OB_SUCCESS != ret) {
      const int unregister_ret = session->stream_registry.unregister(stream_id);
      if (OB_SUCCESS != unregister_ret) {
        LOG_WARN("[FlightSQL] failed to clean up stream after session registration failure",
                 K(unregister_ret), K(stream_id));
      }
      return arrow::Status::IOError(
          std::string("[FlightSQL] failed to register active stream session, ret=")
          + std::to_string(ret));
    }

    // ── 6. Set registry on OB session for resolver/operator access ─
    if (OB_NOT_NULL(session->ob_session)) {
      session->ob_session->set_flight_sql_stream_registry(
          &session->stream_registry);
    }
    // The registry only borrows reader_adapter, which is stack-owned by this
    // handler. Unregister before reader_adapter is destroyed on every exit.
    DEFER(
      if (OB_NOT_NULL(session->ob_session)) {
        session->ob_session->set_flight_sql_stream_registry(nullptr);
      }
      const int unregister_ret = session->stream_registry.unregister(stream_id);
      if (OB_SUCCESS != unregister_ret) {
        LOG_WARN("[FlightSQL] failed to unregister stream",
                 K(unregister_ret), K(stream_id));
      }
      session_mgr_.unregister_active_stream_session(session->free_ctx.sessid_);
    );

    // ── Multi-stream ingest: register local stream, block until VecOp reads it ──
    if (!ingest_id_hdr.empty()) {
      // ":stream" suffix isolates per-observer stream coordination from the
      // coordinator's metadata entry (which stores col names, auto-abort, etc.)
      const std::string stream_key = ingest_id_hdr + ":stream";

      // Create local stream group if this is the first DoPut on this observer.
      ret = ObFlightSqlIngestRegistry::instance().prepare_ingest_with_id(
          session->tenant_id, stream_key, db_name, table_name, streams_per_node);
      if (OB_SUCCESS != ret) {
        return arrow::Status::Invalid(
            std::string("[FlightSQL] DoPut: invalid streams_per_node, ret=")
            + std::to_string(ret));
      }

      std::shared_ptr<ObFlightSqlIngestEntry> local_entry =
          ObFlightSqlIngestRegistry::instance().get(stream_key);
      // A rejected schema must not consume a local slot: a later valid DoPut
      // still needs to be able to complete this observer's stream group.
      if (local_entry) {
        bool schema_matches = true;
        {
          lib::ObMallocHookAttrGuard malloc_guard(
              common::ObMemAttr(local_entry->tenant_id, "FlightSQLIngest"),
              false /* use_500 */);
          std::lock_guard<std::mutex> lk(local_entry->mu);
          if (!local_entry->advertised_arrow_schema) {
            local_entry->advertised_arrow_schema = advertised_ingest_schema;
          } else {
            schema_matches = local_entry->advertised_arrow_schema->Equals(
                *advertised_ingest_schema, false);
          }
          if (!local_entry->local_arrow_schema) {
            local_entry->local_arrow_schema = arrow_schema;
          } else {
            schema_matches = schema_matches
                && local_entry->local_arrow_schema->Equals(*arrow_schema, false);
          }
        }
        if (!schema_matches) {
          return arrow::Status::Invalid(
              "[FlightSQL] DoPut: local streams for one ingest must have identical Arrow schemas");
        }
      }

      // Auto-assign slot within this observer's stream group only after its
      // schema has been accepted.
      const int64_t local_slot = local_entry
          ? local_entry->next_slot.fetch_add(1, std::memory_order_relaxed) : 0;
      if (local_entry && local_slot >= local_entry->parallel) {
        return arrow::Status::Invalid(
            "[FlightSQL] DoPut: too many DoPuts for this observer on ingest_id="
            + ingest_id_hdr);
      }

      const uint32_t sess_id = session->free_ctx.sessid_;
      ret = ObFlightSqlIngestRegistry::instance().register_slot(
          stream_key, local_slot, sess_id, stream_id, session_id);
      if (OB_SUCCESS != ret) {
        return arrow::Status::Invalid(
            std::string("[FlightSQL] DoPut: register_slot failed, ret=")
            + std::to_string(ret));
      }
      LOG_TRACE("[FlightSQL] DoPut: stream registered, waiting for VecOp",
                "ingest_id", ingest_id_hdr.c_str(), K(local_slot));

      cancellation.set_ingest_key(stream_key);
      // Block until VecOp on this observer exhausts the stream (keeps gRPC stream alive).
      static constexpr int64_t INGEST_WAIT_TIMEOUT_US = 600LL * 1000 * 1000;  // 10 min
      const int wait_ret = ObFlightSqlIngestRegistry::instance().wait_stream_exhausted(
          local_entry, INGEST_WAIT_TIMEOUT_US);

      if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
      if (OB_SUCCESS != wait_ret) {
        return arrow::Status::IOError(
            std::string("[FlightSQL] DoPut: stream exhausted with error, ret=")
            + std::to_string(wait_ret));
      }
      LOG_TRACE("[FlightSQL] DoPut: stream exhausted, returning",
                "ingest_id", ingest_id_hdr.c_str());
      if (cancellation.finish()) { return flight_sql_doput_cancelled(); }
      return 0;
    }

    // ── 7. Build SQL ────────────────────────────────────────────
    int write_parallel = 0;
    bool has_write_parallel = false;
    bool direct_load = false;
    bool is_upsert = false;
    bool is_update = false;
    bool is_merge = false;
    bool is_overwrite = false;
    common::ObArenaAllocator options_allocator(
        common::ObMemAttr(session->tenant_id, "FlightSQLIngest"));
    common::ObString merge_options_json;
    ObFlightSqlMergeOptions merge_options(options_allocator, session->tenant_id);
    common::ObString overwrite_options_json;
    bool has_overwrite_options = false;
    std::vector<std::string> overwrite_partitions;
    std::string extra_hint;
    std::vector<std::string> key_cols;   // populated when is_update == true
    std::unordered_set<std::string> update_key_set;
    {
      // Raw Flight SQL clients can carry backend-specific ingest options in
      // CommandStatementIngest.options.  ADBC sends the same options as call
      // headers, so honor both forms and let an explicitly supplied header
      // override the command option for backwards compatibility.
      std::unordered_map<std::string, std::string>::const_iterator option_it =
          command.options.find(FLIGHT_SQL_HEADER_WRITE_PARALLEL);
      if (option_it != command.options.end()) {
        if (OB_FAIL(flight_sql_set_write_parallel_(option_it->second, write_parallel))) {
          return arrow::Status::Invalid(
              "[FlightSQL] ob_write_parallel must be a positive integer");
        }
        has_write_parallel = true;
      }
      option_it = command.options.find(FLIGHT_SQL_HEADER_WRITE_MODE);
      if (option_it != command.options.end()
          && context.incoming_headers().find(FLIGHT_SQL_HEADER_WRITE_MODE)
             == context.incoming_headers().end()) {
        const common::ObString mode_value(
            static_cast<int32_t>(option_it->second.size()), option_it->second.data());
        if (OB_FAIL(flight_sql_set_write_mode_(mode_value, is_upsert, is_update, is_merge, is_overwrite))) {
          return arrow::Status::Invalid(
              "[FlightSQL] ob_write_mode must be insert, upsert, update, merge, or overwrite");
        }
      }
      option_it = command.options.find(FLIGHT_SQL_HEADER_MERGE_OPTIONS);
      if (option_it != command.options.end()) {
        const auto header_it = context.incoming_headers().find(FLIGHT_SQL_HEADER_MERGE_OPTIONS);
        if (header_it == context.incoming_headers().end()) {
          if (option_it->second.size() > 64 * 1024) {
            return arrow::Status::Invalid("[FlightSQL] ob_merge_options must not exceed 64 KiB");
          }
          merge_options_json.assign_ptr(option_it->second.data(), option_it->second.size());
        }
      }
      option_it = command.options.find(FLIGHT_SQL_HEADER_OVERWRITE_OPTIONS);
      if (option_it != command.options.end()) {
        has_overwrite_options = true;
        if (context.incoming_headers().find(FLIGHT_SQL_HEADER_OVERWRITE_OPTIONS)
            == context.incoming_headers().end()) {
          if (option_it->second.size() > 64 * 1024) {
            return arrow::Status::Invalid("[FlightSQL] ob_overwrite_options must not exceed 64 KiB");
          }
          overwrite_options_json.assign_ptr(option_it->second.data(), option_it->second.size());
        }
      }
      option_it = command.options.find(FLIGHT_SQL_HEADER_DIRECT_LOAD);
      if (option_it != command.options.end()) {
        if (OB_FAIL(flight_sql_set_direct_load_(option_it->second, direct_load))) {
          return arrow::Status::Invalid(
              "[FlightSQL] ob_direct_load must be true or false");
        }
      }
      option_it = command.options.find(FLIGHT_SQL_HEADER_WRITE_HINT);
      if (option_it != command.options.end()) {
        if (OB_FAIL(flight_sql_set_write_hint_(option_it->second, extra_hint))) {
          return arrow::Status::Invalid(
              "[FlightSQL] ob_write_hint contains forbidden characters (*/  ;  --)");
        }
      }
      option_it = command.options.find(FLIGHT_SQL_HEADER_WRITE_KEY_COLUMNS);
      if (option_it != command.options.end()) {
        if (OB_FAIL(flight_sql_set_write_key_columns_(option_it->second, key_cols))) {
          return arrow::Status::Invalid(
              "[FlightSQL] key columns must not contain SQL quote or control characters");
        }
      }

      const arrow::flight::CallHeaders &hdrs = context.incoming_headers();
      arrow::flight::CallHeaders::const_iterator it =
          hdrs.find(FLIGHT_SQL_HEADER_WRITE_PARALLEL);
      if (it != hdrs.end()) {
        if (OB_FAIL(flight_sql_set_write_parallel_(
                std::string(it->second), write_parallel))) {
          return arrow::Status::Invalid(
              "[FlightSQL] ob_write_parallel must be a positive integer");
        }
        has_write_parallel = true;
      }
      arrow::flight::CallHeaders::const_iterator it2 =
          hdrs.find(FLIGHT_SQL_HEADER_WRITE_MODE);
      if (it2 != hdrs.end()) {
        const common::ObString mode_value(
            static_cast<int32_t>(it2->second.size()), it2->second.data());
        if (OB_FAIL(flight_sql_set_write_mode_(mode_value, is_upsert, is_update, is_merge, is_overwrite))) {
          return arrow::Status::Invalid(
              "[FlightSQL] ob_write_mode must be insert, upsert, update, merge, or overwrite");
        }
      }
      const arrow::flight::CallHeaders::const_iterator merge_it =
          hdrs.find(FLIGHT_SQL_HEADER_MERGE_OPTIONS);
      if (merge_it != hdrs.end()) {
        if (merge_it->second.size() > 64 * 1024) {
          return arrow::Status::Invalid("[FlightSQL] ob_merge_options must not exceed 64 KiB");
        }
        merge_options_json.assign_ptr(merge_it->second.data(), merge_it->second.size());
      }
      const auto overwrite_it = hdrs.find(FLIGHT_SQL_HEADER_OVERWRITE_OPTIONS);
      if (overwrite_it != hdrs.end()) {
        if (overwrite_it->second.size() > 64 * 1024) {
          return arrow::Status::Invalid("[FlightSQL] ob_overwrite_options must not exceed 64 KiB");
        }
        has_overwrite_options = true;
        overwrite_options_json.assign_ptr(overwrite_it->second.data(), overwrite_it->second.size());
      }
      arrow::flight::CallHeaders::const_iterator direct_load_it =
          hdrs.find(FLIGHT_SQL_HEADER_DIRECT_LOAD);
      if (direct_load_it != hdrs.end()) {
        if (OB_FAIL(flight_sql_set_direct_load_(
                std::string(direct_load_it->second), direct_load))) {
          return arrow::Status::Invalid(
              "[FlightSQL] ob_direct_load must be true or false");
        }
      }
      arrow::flight::CallHeaders::const_iterator it3 =
          hdrs.find(FLIGHT_SQL_HEADER_WRITE_HINT);
      if (it3 != hdrs.end()) {
        if (OB_FAIL(flight_sql_set_write_hint_(std::string(it3->second), extra_hint))) {
          return arrow::Status::Invalid(
              "[FlightSQL] ob_write_hint contains forbidden characters (*/  ;  --)");
        }
      }
      arrow::flight::CallHeaders::const_iterator it4 =
          hdrs.find(FLIGHT_SQL_HEADER_WRITE_KEY_COLUMNS);
      if (it4 != hdrs.end()) {
        if (OB_FAIL(flight_sql_set_write_key_columns_(std::string(it4->second), key_cols))) {
          return arrow::Status::Invalid(
              "[FlightSQL] key columns must not contain SQL quote or control characters");
        }
      }
    }
    if (has_overwrite_options && !is_overwrite) {
      return arrow::Status::Invalid(
          "[FlightSQL] ob_overwrite_options is only valid with ob_write_mode=overwrite");
    }
    if (is_overwrite) {
      const auto &hdrs = context.incoming_headers();
      if (command.options.find(FLIGHT_SQL_HEADER_WRITE_KEY_COLUMNS) != command.options.end()
          || hdrs.find(FLIGHT_SQL_HEADER_WRITE_KEY_COLUMNS) != hdrs.end()
          || command.options.find(FLIGHT_SQL_HEADER_MERGE_OPTIONS) != command.options.end()
          || hdrs.find(FLIGHT_SQL_HEADER_MERGE_OPTIONS) != hdrs.end()) {
        return arrow::Status::Invalid(
            "[FlightSQL] overwrite mode cannot use ob_write_key_columns or ob_merge_options");
      }
      if (has_overwrite_options) {
        ARROW_RETURN_NOT_OK(flight_sql_parse_overwrite_options_(
            overwrite_options_json, options_allocator, is_oracle_mode, overwrite_partitions));
      }
      // Native INSERT OVERWRITE validates transaction state and direct-load
      // hints, and owns replacement/publication for the entire input stream.
    }
    if (is_merge) {
      if (direct_load || !key_cols.empty()) {
        return arrow::Status::Invalid(
            "[FlightSQL] merge mode cannot use direct_load or ob_write_key_columns");
      }
      if (OB_ISNULL(session->ob_session)) {
        return arrow::Status::Invalid("[FlightSQL] merge mode requires an SQL session");
      }
      common::ObString error;
      if (OB_FAIL(merge_options.parse(merge_options_json, error))) {
        return arrow::Status::Invalid("[FlightSQL] ", std::string(error.ptr(), error.length()));
      }
      // The DoPut thread has not entered the SQL engine's tenant scope yet.
      // Expression parsing must use the authenticated session's dialect.
      lib::CompatModeGuard compat_guard(session->ob_session->is_oracle_compatible()
          ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
      if (OB_FAIL(merge_options.validate_expressions(
                       session->ob_session->get_sql_mode(),
                       session->ob_session->get_charsets4parser(), error))) {
        return arrow::Status::Invalid("[FlightSQL] ", std::string(error.ptr(), error.length()));
      }
      ObArrowStreamEntryPtr stream_entry = session->stream_registry.get(stream_id);
      if (OB_ISNULL(stream_entry)) {
        return arrow::Status::Invalid("[FlightSQL] merge source stream was not found");
      }
      stream_entry->is_merge_source = true;
    } else if (!merge_options_json.empty()) {
      return arrow::Status::Invalid(
          "[FlightSQL] ob_merge_options is only valid with ob_write_mode=merge");
    }
    if (direct_load && is_upsert) {
      return arrow::Status::Invalid(
          "[FlightSQL] direct_load mode does not support upsert "
          "(ON DUPLICATE KEY UPDATE). Use append mode or disable direct_load.");
    }
    if (is_update && (direct_load || is_upsert)) {
      return arrow::Status::Invalid(
          "[FlightSQL] update mode cannot be combined with direct_load or upsert.");
    }
    if (is_update && key_cols.empty()) {
      return arrow::Status::Invalid(
          "[FlightSQL] update mode requires ob_write_key_columns option or header "
          "(comma-separated list of join key column names).");
    }
    if (is_update) {
      const arrow::Status key_status = flight_sql_validate_update_key_columns_(
          col_names, key_cols, update_key_set);
      if (!key_status.ok()) {
        return key_status;
      }
    }

    // ── Build SQL ──────────────────────────────────────────────────────────
    std::string sql;
    common::ObSqlString merge_sql;
    merge_sql.set_attr(common::ObMemAttr(session->tenant_id, "FlightSQLMerge"));

    if (is_merge) {
      const common::ObString merge_db(db_name.size(), db_name.data());
      const common::ObString merge_table(table_name.size(), table_name.data());
      const common::ObString merge_hint(extra_hint.size(), extra_hint.data());
      if (OB_NOT_NULL(merge_db.find('"')) || OB_NOT_NULL(merge_table.find('"'))) {
        return arrow::Status::Invalid("[FlightSQL] merge target names must not contain SQL quotes");
      } else if (OB_FAIL(merge_options.build_sql(merge_db, merge_table, stream_id,
          has_write_parallel, write_parallel, merge_hint,
          session->ob_session->is_oracle_compatible(), merge_sql))) {
        return arrow::Status::OutOfMemory("[FlightSQL] failed to build merge SQL");
      }
    } else if (is_update) {
      sql.reserve(512 + col_names.size() * 64);
      // UPDATE `db`.`table` t, TABLE(arrow_stream(X)) s
      // SET t.`c1` = s.`c1`, ...
      // WHERE t.`k` = s.`k` AND ...
      // Non-matching rows are silently skipped (INNER JOIN semantics).
      sql.append("UPDATE ");
      if (has_write_parallel || !extra_hint.empty()) {
        sql.append("/*+");
        if (has_write_parallel) {
          sql.append(" ENABLE_PARALLEL_DML PARALLEL(");
          sql.append(std::to_string(write_parallel));
          sql.push_back(')');
        }
        if (!extra_hint.empty()) {
          sql.push_back(' ');
          sql.append(extra_hint);
        }
        sql.append(" */ ");
      }
      flight_sql_append_identifier(sql, db_name, is_oracle_mode);
      sql.push_back('.');
      flight_sql_append_identifier(sql, table_name, is_oracle_mode);
      sql.append(" t, TABLE(arrow_stream(");
      sql.append(std::to_string(stream_id));
      sql.append(")) s SET ");
      bool first_set = true;
      for (const std::string &col : col_names) {
        if (update_key_set.count(col)) continue;   // skip key columns in SET
        if (!first_set) { sql.append(", "); }
        sql.append("t.");
        flight_sql_append_identifier(sql, col, is_oracle_mode);
        sql.append(" = s.");
        flight_sql_append_identifier(sql, col, is_oracle_mode);
        first_set = false;
      }
      if (first_set) {
        return arrow::Status::Invalid(
            "[FlightSQL] update mode: no non-key columns to update "
            "(all columns are listed as key columns).");
      }
      sql.append(" WHERE ");
      bool first_key = true;
      for (const std::string &kc : key_cols) {
        if (!first_key) { sql.append(" AND "); }
        sql.append("t.");
        flight_sql_append_identifier(sql, kc, is_oracle_mode);
        sql.append(" = s.");
        flight_sql_append_identifier(sql, kc, is_oracle_mode);
        first_key = false;
      }
    } else {
      sql.reserve(512 + col_names.size() * 64);
      sql.append("INSERT ");
      sql.append("/*+");
      // OVERWRITE selects full direct load in the native optimizer itself.
      if (direct_load && !is_overwrite) {
        sql.append(" APPEND");
      }
      sql.append(" ENABLE_PARALLEL_DML");
      if (has_write_parallel) {
        sql.append(" PARALLEL(");
        sql.append(std::to_string(write_parallel));
        sql.append(")");
      }
      if (!extra_hint.empty()) {
        sql.push_back(' ');
        sql.append(extra_hint);
      }
      sql.append(" */ ");
      if (is_overwrite) {
        sql.append("OVERWRITE ");
      }
      sql.append("INTO ");
    if (!catalog_name.empty()
        && 0 != strcasecmp(catalog_name.c_str(), OB_INTERNAL_CATALOG_NAME)) {
      flight_sql_append_identifier(sql, catalog_name, is_oracle_mode);
      sql.push_back('.');
    }
    flight_sql_append_identifier(sql, db_name, is_oracle_mode);
    sql.push_back('.');
    flight_sql_append_identifier(sql, table_name, is_oracle_mode);
    if (!overwrite_partitions.empty()) {
      sql.append(" PARTITION (");
      for (size_t i = 0; i < overwrite_partitions.size(); ++i) {
        if (i > 0) { sql.append(", "); }
        flight_sql_append_identifier(sql, overwrite_partitions[i], is_oracle_mode);
      }
      sql.push_back(')');
    }
    sql.push_back('(');
    for (size_t i = 0; i < col_names.size(); ++i) {
      if (i > 0) { sql.append(", "); }
      flight_sql_append_identifier(sql, col_names[i], is_oracle_mode);
    }
    sql.append(") SELECT ");
    for (size_t i = 0; i < col_names.size(); ++i) {
      if (i > 0) { sql.append(", "); }
      sql.append("s.");
      flight_sql_append_identifier(sql, col_names[i], is_oracle_mode);
    }
    sql.append(" FROM TABLE(arrow_stream(");
    sql.append(std::to_string(stream_id));
    sql.append(")) s");

    if (is_upsert) {
      sql.append(" ON DUPLICATE KEY UPDATE ");
      for (size_t i = 0; i < col_names.size(); ++i) {
        if (i > 0) { sql.append(", "); }
        flight_sql_append_identifier(sql, col_names[i], is_oracle_mode);
        sql.append("=VALUES(");
        flight_sql_append_identifier(sql, col_names[i], is_oracle_mode);
        sql.push_back(')');
      }
    }   // end if (is_upsert)
    }   // end INSERT else-branch (closes } else { at is_update)

    const common::ObString ob_sql = is_merge ? merge_sql.string()
        : common::ObString(static_cast<int32_t>(sql.size()), sql.data());
    LOG_TRACE("[FlightSQL] DoPutCommandStatementIngest: executing SQL",
              "stream_id", static_cast<int64_t>(stream_id),
              "mode", is_overwrite ? "overwrite" : (is_merge ? "merge" : (is_update ? "update" : (is_upsert ? "upsert" : "append"))),
              "tls_registry", (void*)&session->stream_registry,
              K(ob_sql));

    // ── 8. Execute generated DML via SQL engine ─────────────────
    ObFlightSqlQueryProcessor proc;
    const int exec_ret = proc.execute(
        *session, ob_sql, "DoPutCommandStatementIngest", true, cancellation.request().get());

    if (OB_SUCCESS != exec_ret) {
      if (cancellation.is_cancelled()) { return flight_sql_doput_cancelled(); }
      const std::string sql_preview(
          ob_sql.ptr(), std::min(int32_t(200), ob_sql.length()));
      const std::string detail_msg =
          flight_sql_ingest_error_message(exec_ret, arrow_schema);
      return ob_sql_exec_ret_to_arrow_status(
          exec_ret, proc.trace_id().c_str(), sql_preview, &detail_msg);
    }

    const int64_t affected = proc.affected_rows();
    LOG_TRACE("[FlightSQL] DoPutCommandStatementIngest: OK",
              "stream_id", static_cast<int64_t>(stream_id),
              "affected_rows", affected,
              "trace_id", proc.trace_id().c_str());
    if (cancellation.finish()) { return flight_sql_doput_cancelled(); }
    return affected;

    } OB_FLIGHT_SQL_CATCH_AND_REMOVE_SESSION
  }

  // ============================================================
  // Metadata — not needed this release
  // ============================================================

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoCatalogs(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::FlightDescriptor &) override
  { return arrow::Status::NotImplemented("Catalogs not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetCatalogs(
      const arrow::flight::ServerCallContext &) override
  { return arrow::Status::NotImplemented("Catalogs not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoSchemas(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetDbSchemas &,
      const arrow::flight::FlightDescriptor &) override
  { return arrow::Status::NotImplemented("DbSchemas not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetDbSchemas(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetDbSchemas &) override
  { return arrow::Status::NotImplemented("DbSchemas not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoTables(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetTables &,
      const arrow::flight::FlightDescriptor &) override
  { return arrow::Status::NotImplemented("Tables not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetTables(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetTables &) override
  { return arrow::Status::NotImplemented("Tables not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoTableTypes(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::FlightDescriptor &) override
  { return arrow::Status::NotImplemented("TableTypes not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetTableTypes(
      const arrow::flight::ServerCallContext &) override
  { return arrow::Status::NotImplemented("TableTypes not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoPrimaryKeys(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetPrimaryKeys &,
      const arrow::flight::FlightDescriptor &) override
  { return arrow::Status::NotImplemented("PrimaryKeys not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetPrimaryKeys(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetPrimaryKeys &) override
  { return arrow::Status::NotImplemented("PrimaryKeys not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoExportedKeys(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetExportedKeys &,
      const arrow::flight::FlightDescriptor &) override
  { return arrow::Status::NotImplemented("ExportedKeys not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetExportedKeys(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetExportedKeys &) override
  { return arrow::Status::NotImplemented("ExportedKeys not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoImportedKeys(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetImportedKeys &,
      const arrow::flight::FlightDescriptor &) override
  { return arrow::Status::NotImplemented("ImportedKeys not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetImportedKeys(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetImportedKeys &) override
  { return arrow::Status::NotImplemented("ImportedKeys not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoCrossReference(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetCrossReference &,
      const arrow::flight::FlightDescriptor &) override
  { return arrow::Status::NotImplemented("CrossReference not supported"); }

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetCrossReference(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::GetCrossReference &) override
  { return arrow::Status::NotImplemented("CrossReference not supported"); }

  // ============================================================
  // Transactions — not supported; GetSqlInfo reports NONE so compliant
  // clients (ADBC >= 1.1) will not call these.
  // ============================================================

  arrow::Result<arrow::flight::sql::ActionBeginTransactionResult> BeginTransaction(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::ActionBeginTransactionRequest &) override
  { return arrow::Status::NotImplemented("Transactions not supported"); }

  arrow::Status EndTransaction(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::ActionEndTransactionRequest &) override
  { return arrow::Status::NotImplemented("Transactions not supported"); }

  arrow::Result<arrow::flight::sql::ActionBeginSavepointResult> BeginSavepoint(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::ActionBeginSavepointRequest &) override
  { return arrow::Status::NotImplemented("Savepoints not supported"); }

  arrow::Status EndSavepoint(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::sql::ActionEndSavepointRequest &) override
  { return arrow::Status::NotImplemented("Savepoints not supported"); }

  // ============================================================
  // Cancel
  // ============================================================

  arrow::Result<arrow::flight::sql::CancelResult> CancelQuery(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::ActionCancelQueryRequest &) override
  {
    try {
    const std::string session_id = context.peer_identity();
    LOG_TRACE("[FlightSQL] CancelQuery: action received",
             "peer", context.peer().c_str(),
             "session_id", session_id.c_str());

    int ret = OB_SUCCESS;
    std::shared_ptr<ObFlightSqlSession> session = session_mgr_.get_live(session_id);
    if (!session) {
      LOG_TRACE("[FlightSQL] CancelQuery: flight session not found (no kill)",
               "session_id", session_id.c_str());
      LOG_WARN("[FlightSQL] CancelQuery: session not found",
               "session_id", session_id.c_str());
      return arrow::flight::sql::CancelResult::kUnspecified;
    }

    std::shared_ptr<ObFlightSqlRequestCancellation> cancellation;
    bool admitted_doput = false;
    {
      std::lock_guard<std::mutex> guard(session->mutex);
      admitted_doput = session->doput_active_;
      if (admitted_doput) { session->doput_cancel_pending_ = true; }
      cancellation = session->active_doput_cancellation;
    }
    if (cancellation) {
      return cancellation->cancel([] {})
          ? arrow::flight::sql::CancelResult::kCancelling
          : arrow::flight::sql::CancelResult::kCancelled;
    } else if (admitted_doput) {
      // The admitted handler will apply the sticky cancellation when it
      // publishes its request. Do not issue an unfenced late SQL kill here.
      return arrow::flight::sql::CancelResult::kCancelling;
    }

    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    if (OB_FAIL(tenant_guard.switch_to(session->tenant_id))) {
      LOG_WARN("[FlightSQL] CancelQuery tenant switch failed", K(ret));
      return arrow::flight::sql::CancelResult::kUnspecified;
    }
    uint32_t server_sessid = sql::ObSQLSessionInfo::INVALID_SESSID;
    {
      std::lock_guard<std::mutex> guard(session->mutex);
      if (OB_NOT_NULL(session->ob_session)) {
        server_sessid = session->free_ctx.sessid_;
      }
    }
    if (sql::ObSQLSessionInfo::INVALID_SESSID == server_sessid || OB_ISNULL(GCTX.session_mgr_)) {
      LOG_TRACE("[FlightSQL] CancelQuery: skip kill (no ob sessid or session_mgr null)",
               "session_id", session_id.c_str(),
               K(server_sessid),
               "session_mgr_null", OB_ISNULL(GCTX.session_mgr_) ? 1 : 0);
      LOG_WARN("[FlightSQL] CancelQuery: no server session or session_mgr",
               "session_id", session_id.c_str());
      return arrow::flight::sql::CancelResult::kUnspecified;
    }
    {
      sql::ObSQLSessionInfo *sess = nullptr;
      sql::ObSessionGetterGuard guard(*GCTX.session_mgr_, server_sessid);
      ret = guard.get_session(sess);
      if (OB_FAIL(ret) || OB_ISNULL(sess)) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_TRACE("[FlightSQL] CancelQuery: get_session failed, no kill_query",
                 K(ret), "session_id", session_id.c_str(), K(server_sessid));
        LOG_WARN("[FlightSQL] CancelQuery: get_session failed", K(ret), "session_id", session_id.c_str(),
                 K(server_sessid));
      } else if (OB_FAIL(ret = GCTX.session_mgr_->kill_query(*sess))) {
        LOG_TRACE("[FlightSQL] CancelQuery: kill_query returned error",
                 K(ret), "session_id", session_id.c_str(), K(server_sessid));
        LOG_WARN("[FlightSQL] CancelQuery: kill_query failed", K(ret), K(server_sessid));
      } else {
        LOG_TRACE("[FlightSQL] CancelQuery: kill_query sent (QUERY_KILLED)",
                 "session_id", session_id.c_str(),
                 K(server_sessid),
                 "peer", context.peer().c_str());
      }
    }

    if (OB_SUCCESS == ret) {
      LOG_TRACE("[FlightSQL] CancelQuery: returning kCancelled", K(ret), K(server_sessid));
    } else {
      LOG_TRACE("[FlightSQL] CancelQuery failed: returning kUnspecified", K(ret), K(server_sessid));
    }
    return OB_SUCCESS == ret ? arrow::flight::sql::CancelResult::kCancelled
                             : arrow::flight::sql::CancelResult::kUnspecified;
    } OB_FLIGHT_SQL_CATCH_CANCEL_QUERY
  }


  // ============================================================
  // V2 ingest dispatch (called from DoGetStatement)
  // ============================================================

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
  dispatch_ingest_action(ObFlightSqlSession &session,
                         const std::string &action,
                         const std::string &body_json)
  {
    LOG_TRACE("[FlightSQL] dispatch_ingest_action",
              "action", action.c_str());
    const bool is_oracle_mode = OB_NOT_NULL(session.ob_session)
        && session.ob_session->is_oracle_compatible();

    if (action == "prepare_ingest") {
      const std::string db       = ob_json_str(body_json, "db");
      const std::string table    = ob_json_str(body_json, "table");
      const int64_t    parallel  = ob_json_int(body_json, "parallel", 1);
      const std::vector<std::string> servers = ob_json_str_array(body_json, "servers");
      if (db.empty() || table.empty()) {
        return arrow::Status::Invalid("[FlightSQL] prepare_ingest: db and table are required");
      }
      if (!flight_sql_is_safe_identifier(db) || !flight_sql_is_safe_identifier(table)) {
        return arrow::Status::Invalid(
            "[FlightSQL] prepare_ingest: db and table must not contain SQL quote or control characters");
      }
      if (is_oracle_mode
          && (!flight_sql_is_safe_oracle_identifier(db)
              || !flight_sql_is_safe_oracle_identifier(table))) {
        return arrow::Status::Invalid(
            "[FlightSQL] prepare_ingest: Oracle mode requires regular unquoted db and table identifiers");
      }
      std::string ingest_id;
      const int ret = ObFlightSqlIngestRegistry::instance().prepare_ingest(
          session.tenant_id, db, table, parallel, ingest_id);
      if (OB_SUCCESS != ret) {
        return arrow::Status::Invalid(
            "[FlightSQL] prepare_ingest failed, ret=" + std::to_string(ret));
      }
      if (!servers.empty()) {
        std::shared_ptr<ObFlightSqlIngestEntry> entry =
            ObFlightSqlIngestRegistry::instance().get(ingest_id);
        if (entry) {
          // Map client's flight_sql addresses to OB RPC addresses for PX scheduling.
          // Query GV$OB_PARAMETERS through the sys SQL proxy to get flight_sql_port for
          // each observer.  This is server metadata; do not route it through the user's
          // Flight SQL result/Arrow conversion path.
          std::unordered_map<std::string, std::string> flight_to_rpc;
          static const char *const map_sql =
              "SELECT /*+NO_USE_PX NO_REWRITE NO_PARALLEL*/ svr_ip, svr_port, value "
              "FROM oceanbase.GV$OB_PARAMETERS"
              " WHERE name = 'flight_sql_port'";
          int map_ret = OB_SUCCESS;
          {
            common::ObMySQLProxy::MySQLResult map_result;
            common::sqlclient::ObMySQLResult *result = nullptr;
            const int64_t request_timeout_ts =
                ObFlightSqlQueryProcessor::start_request_timeout(session);
            lib::WorkerTimeoutGuard request_timeout_guard(request_timeout_ts);
            if (OB_ISNULL(GCTX.sql_proxy_)) {
              map_ret = OB_NOT_INIT;
              LOG_WARN("[FlightSQL] prepare_ingest: sql proxy is null", K(map_ret));
            } else if (OB_SUCCESS != (map_ret = GCTX.sql_proxy_->read(
                           map_result, common::OB_SYS_TENANT_ID, map_sql))) {
              LOG_WARN("[FlightSQL] prepare_ingest: failed to query flight_sql_port mapping",
                       K(map_ret));
            } else if (OB_ISNULL(result = map_result.get_result())) {
              map_ret = OB_ERR_UNEXPECTED;
              LOG_WARN("[FlightSQL] prepare_ingest: sql proxy returned null result", K(map_ret));
            } else {
              while (OB_SUCCESS == map_ret) {
                if (OB_SUCCESS != (map_ret = result->next())) {
                  if (OB_ITER_END == map_ret) {
                    map_ret = OB_SUCCESS;
                    break;
                  }
                  LOG_WARN("[FlightSQL] prepare_ingest: iterate flight_sql_port mapping failed",
                           K(map_ret));
                } else {
                  common::ObString svr_ip;
                  common::ObString flight_port;
                  int64_t svr_port = 0;
                  if (OB_SUCCESS != (map_ret = result->get_varchar("svr_ip", svr_ip))) {
                    LOG_WARN("[FlightSQL] prepare_ingest: get svr_ip failed", K(map_ret));
                  } else if (OB_SUCCESS != (map_ret = result->get_int("svr_port", svr_port))) {
                    LOG_WARN("[FlightSQL] prepare_ingest: get svr_port failed", K(map_ret));
                  } else if (OB_SUCCESS != (map_ret = result->get_varchar("value", flight_port))) {
                    LOG_WARN("[FlightSQL] prepare_ingest: get flight_sql_port failed", K(map_ret));
                  } else {
                    const std::string ip(svr_ip.ptr(), svr_ip.length());
                    const std::string rpc_port = std::to_string(svr_port);
                    const std::string fport(flight_port.ptr(), flight_port.length());
                    flight_to_rpc[ip + ":" + fport] = ip + ":" + rpc_port;
                    LOG_DEBUG("[FlightSQL] prepare_ingest: mapping row",
                              "ip", ip.c_str(), "rpc_port", rpc_port.c_str(),
                              "flight_port", fport.c_str());
                  }
                }
              }
            }
          }
          if (OB_SUCCESS != map_ret) {
            ObFlightSqlIngestRegistry::instance().remove(ingest_id);
            return ob_sql_exec_ret_to_arrow_status(map_ret, nullptr, map_sql);
          }
          // Translate servers list: flight addrs → RPC addrs.
          std::vector<std::string> rpc_addrs;
          for (const std::string &flight_addr : servers) {
            std::unordered_map<std::string, std::string>::const_iterator it =
                flight_to_rpc.find(flight_addr);
            if (it != flight_to_rpc.end()) {
              rpc_addrs.push_back(it->second);
            } else {
              LOG_WARN("[FlightSQL] prepare_ingest: reject unmapped flight addr",
                       "addr", flight_addr.c_str());
              ObFlightSqlIngestRegistry::instance().remove(ingest_id);
              return arrow::Status::Invalid(
                  "[FlightSQL] prepare_ingest: server is not a known Flight SQL observer: "
                  + flight_addr);
            }
          }
          if (rpc_addrs.size() != servers.size()) {
            ObFlightSqlIngestRegistry::instance().remove(ingest_id);
            return arrow::Status::Invalid(
                "[FlightSQL] prepare_ingest: mapped RPC address count does not match "
                "the input Flight address count");
          }
          {
            lib::ObMallocHookAttrGuard malloc_guard(
                common::ObMemAttr(entry->tenant_id, "FlightSQLIngest"),
                false /* use_500 */);
            entry->server_list = rpc_addrs;
          }
          for (size_t ri = 0; ri < rpc_addrs.size(); ++ri) {
            LOG_DEBUG("[FlightSQL] prepare_ingest: rpc_addr",
                      "idx", ri,
                      "flight_addr", servers[ri].c_str(),
                      "rpc_addr", rpc_addrs[ri].c_str());
          }
          LOG_TRACE("[FlightSQL] prepare_ingest: server mapping complete",
                    "ingest_id", ingest_id.c_str(),
                    "flight_count", servers.size(),
                    "rpc_count", rpc_addrs.size());
        }
      }
      // Store for auto-abort on session death.
      {
        lib::ObMallocHookAttrGuard malloc_guard(
            common::ObMemAttr(session.tenant_id, "FlightSQLSess"),
            false /* use_500 */);
        std::lock_guard<std::mutex> g(session.mutex);
        session.last_ingest_id = ingest_id;
      }
      const std::string result = "{\"ingest_id\":\"" + ingest_id + "\"}";
      return ob_make_ingest_result_stream("prepare_ingest", result);

    } else if (action == "execute_ingest") {
      const std::string ingest_id  = ob_json_str(body_json, "ingest_id");
      const std::string write_mode = ob_json_str(body_json, "write_mode");
      const bool direct_load       = ob_json_bool(body_json, "direct_load", false);
      const bool has_parallel      = ob_json_has_key(body_json, "parallel");
      const int64_t parallel       = ob_json_int(body_json, "parallel", 0);
      const std::string key_cols_s = ob_json_str(body_json, "key_columns");
      const std::string extra_hint = ob_json_str(body_json, "extra_hints");
      // The exact Arrow IPC schema supplies the function-table column names
      // and types before SQL resolution.  The coordinator must not wait for a
      // remote DoPut merely to discover them.
      const std::string arrow_schema_s = ob_json_str(body_json, "arrow_schema");
      if (ingest_id.empty()) {
        return arrow::Status::Invalid("[FlightSQL] execute_ingest: ingest_id is required");
      }

      std::shared_ptr<ObFlightSqlIngestEntry> ingest_entry =
          ObFlightSqlIngestRegistry::instance().get(ingest_id);
      if (!ingest_entry) {
        return arrow::Status::Invalid("[FlightSQL] execute_ingest: ingest_id not found");
      }

      std::shared_ptr<arrow::Schema> advertised_arrow_schema;
      if (!arrow_schema_s.empty()) {
        arrow::Result<std::shared_ptr<arrow::Schema>> schema_result =
            ob_decode_ingest_arrow_schema(arrow_schema_s);
        if (!schema_result.ok()) {
          return schema_result.status();
        }
        advertised_arrow_schema = schema_result.MoveValueUnsafe();
      }
      if (OB_ISNULL(advertised_arrow_schema)) {
        return arrow::Status::Invalid(
            "[FlightSQL] execute_ingest: arrow_schema is required");
      }
      std::vector<std::string> advertised_col_names;
      advertised_col_names.reserve(
          static_cast<size_t>(advertised_arrow_schema->num_fields()));
      for (int i = 0; i < advertised_arrow_schema->num_fields(); ++i) {
        const std::shared_ptr<arrow::Field> field = advertised_arrow_schema->field(i);
        if (OB_ISNULL(field) || !flight_sql_is_safe_identifier(field->name())) {
          return arrow::Status::Invalid(
              "[FlightSQL] execute_ingest: Arrow field names must be safe identifiers");
        }
        if (is_oracle_mode
            && !flight_sql_is_safe_oracle_identifier(field->name())) {
          return arrow::Status::Invalid(
              "[FlightSQL] execute_ingest: Oracle mode requires regular unquoted Arrow field names");
        }
        advertised_col_names.push_back(field->name());
      }
      if (advertised_arrow_schema->num_fields() <= 0) {
        return arrow::Status::Invalid("[FlightSQL] execute_ingest: Arrow schema is empty");
      }
      if (!extra_hint.empty() && !flight_sql_is_safe_hint(extra_hint)) {
        return arrow::Status::Invalid(
            "[FlightSQL] execute_ingest: extra_hints contains forbidden characters (*/  ;  --)");
      }
      if (!write_mode.empty() && write_mode != "insert" && write_mode != "upsert"
          && write_mode != "update") {
        return arrow::Status::Invalid(
            "[FlightSQL] execute_ingest: write_mode must be insert, upsert, or update");
      }
      if (has_parallel && parallel <= 0) {
        return arrow::Status::Invalid(
            "[FlightSQL] execute_ingest: parallel must be a positive integer");
      }

      // Store the resolver input atomically.  A retry must not replace a
      // schema that an already-running V3 plan is using.
      {
        lib::ObMallocHookAttrGuard malloc_guard(
            common::ObMemAttr(ingest_entry->tenant_id, "FlightSQLIngest"),
            false /* use_500 */);
        std::lock_guard<std::mutex> lock(ingest_entry->mu);
        if (OB_NOT_NULL(advertised_arrow_schema)
            && OB_NOT_NULL(ingest_entry->advertised_arrow_schema)
            && !ingest_entry->advertised_arrow_schema->Equals(
                *advertised_arrow_schema, false)) {
          return arrow::Status::Invalid(
              "[FlightSQL] execute_ingest: Arrow schema differs from prior request");
        }
        if (OB_NOT_NULL(advertised_arrow_schema)) {
          ingest_entry->advertised_arrow_schema = advertised_arrow_schema;
        }
      }

      // CommitIngest fires SQL immediately — VecOp on each node waits locally
      // for its stream via wait_for_stream(). No coordinator-side wait needed.

      // Parse key columns for update mode.
      std::vector<std::string> key_cols;
      if (!key_cols_s.empty()) {
        std::stringstream ss(key_cols_s);
        std::string col;
        while (std::getline(ss, col, ',')) {
          const std::string::size_type s = col.find_first_not_of(" \t");
          const std::string::size_type e = col.find_last_not_of(" \t");
          if (s != std::string::npos) {
            const std::string key_col = col.substr(s, e - s + 1);
            if (!flight_sql_is_safe_identifier(key_col)) {
              return arrow::Status::Invalid(
                  "[FlightSQL] execute_ingest: key columns must not contain SQL quote or control characters");
            }
            key_cols.push_back(key_col);
          }
        }
      }

      const bool is_upsert = (write_mode == "upsert");
      const bool is_update = (write_mode == "update");
      std::unordered_set<std::string> update_key_set;
      const std::string &db    = ingest_entry->db_name;
      const std::string &table = ingest_entry->table_name;

      if (direct_load && (is_upsert || is_update)) {
        return arrow::Status::Invalid(
            "[FlightSQL] execute_ingest: direct_load only supports write_mode=insert");
      }
      if (is_update && key_cols.empty()) {
        return arrow::Status::Invalid(
            "[FlightSQL] execute_ingest: update mode requires key_columns");
      }
      if (is_update) {
        const arrow::Status key_status = flight_sql_validate_update_key_columns_(
            advertised_col_names, key_cols, update_key_set);
        if (!key_status.ok()) {
          return key_status;
        }
      }

      // Generate SQL using TABLE(arrow_streams('ingest_id')).
      std::string sql;
      sql.reserve(512 + static_cast<size_t>(advertised_arrow_schema->num_fields()) * 64);
      if (is_update) {
        sql = "UPDATE ";
        if (has_parallel || !extra_hint.empty()) {
          sql += "/*+";
        }
        if (has_parallel) {
          sql += " ENABLE_PARALLEL_DML PARALLEL(" + std::to_string(parallel) + ")";
        }
        if (!extra_hint.empty()) { sql += " " + extra_hint; }
        if (has_parallel || !extra_hint.empty()) { sql += " */ "; }
        flight_sql_append_identifier(sql, db, is_oracle_mode);
        sql += ".";
        flight_sql_append_identifier(sql, table, is_oracle_mode);
        sql += " t, TABLE(arrow_streams('" + ingest_id + "')) s SET ";
        bool first = true;
        for (int i = 0; i < advertised_arrow_schema->num_fields(); ++i) {
          const std::string &c = advertised_arrow_schema->field(i)->name();
          if (update_key_set.count(c)) { continue; }
          if (!first) { sql += ", "; }
          sql += "t.";
          flight_sql_append_identifier(sql, c, is_oracle_mode);
          sql += " = s.";
          flight_sql_append_identifier(sql, c, is_oracle_mode);
          first = false;
        }
        sql += " WHERE ";
        bool first_key = true;
        for (const std::string &kc : key_cols) {
          if (!first_key) { sql += " AND "; }
          sql += "t.";
          flight_sql_append_identifier(sql, kc, is_oracle_mode);
          sql += " = s.";
          flight_sql_append_identifier(sql, kc, is_oracle_mode);
          first_key = false;
        }
      } else {
        sql = "INSERT ";
        sql += "/*+";
        if (direct_load) { sql += " APPEND"; }
        sql += " ENABLE_PARALLEL_DML";
        if (has_parallel) { sql += " PARALLEL(" + std::to_string(parallel) + ")"; }
        if (!extra_hint.empty()) { sql += " " + extra_hint; }
        sql += " */ ";
        sql += "INTO ";
        flight_sql_append_identifier(sql, db, is_oracle_mode);
        sql += ".";
        flight_sql_append_identifier(sql, table, is_oracle_mode);
        sql += "(";
        for (int i = 0; i < advertised_arrow_schema->num_fields(); ++i) {
          if (i) { sql += ", "; }
          flight_sql_append_identifier(
              sql, advertised_arrow_schema->field(i)->name(), is_oracle_mode);
        }
        sql += ") SELECT ";
        for (int i = 0; i < advertised_arrow_schema->num_fields(); ++i) {
          if (i) { sql += ", "; }
          sql += "s.";
          flight_sql_append_identifier(
              sql, advertised_arrow_schema->field(i)->name(), is_oracle_mode);
        }
        sql += " FROM TABLE(arrow_streams('" + ingest_id + "')) s";
        if (is_upsert) {
          sql += " ON DUPLICATE KEY UPDATE ";
          for (int i = 0; i < advertised_arrow_schema->num_fields(); ++i) {
            if (i) { sql += ", "; }
            const std::string &field_name = advertised_arrow_schema->field(i)->name();
            flight_sql_append_identifier(sql, field_name, is_oracle_mode);
            sql += "=VALUES(";
            flight_sql_append_identifier(sql, field_name, is_oracle_mode);
            sql += ")";
          }
        }
      }

      LOG_TRACE("[FlightSQL] execute_ingest: executing SQL",
                "ingest_id", ingest_id.c_str(), "sql_preview",
                sql.substr(0, std::min(size_t(200), sql.size())).c_str());

      {
        std::shared_ptr<ObFlightSqlIngestEntry> entry =
            ObFlightSqlIngestRegistry::instance().get(ingest_id);
        if (OB_NOT_NULL(entry)) {
          std::lock_guard<std::mutex> lk(entry->mu);
          entry->insert_session = session.ob_session;
        }
      }

      ObFlightSqlQueryProcessor proc;
      const common::ObString ob_sql(static_cast<int32_t>(sql.size()), sql.c_str());
      int ret = proc.execute(session, ob_sql, "DoGetStatement");
      const int64_t affected = proc.affected_rows();

      {
        std::shared_ptr<ObFlightSqlIngestEntry> entry =
            ObFlightSqlIngestRegistry::instance().get(ingest_id);
        if (OB_NOT_NULL(entry)) {
          std::lock_guard<std::mutex> lk(entry->mu);
          entry->insert_session = nullptr;
        }
      }
      // VecOp on each node signals stream_exhausted and removes its local entry.
      {
        lib::ObMallocHookAttrGuard malloc_guard(
            common::ObMemAttr(session.tenant_id, "FlightSQLSess"),
            false /* use_500 */);
        std::lock_guard<std::mutex> g(session.mutex);
        session.last_ingest_id.clear();
      }
      // Remove coordinator's entry (coordinator may have no local slot in V3).
      ObFlightSqlIngestRegistry::instance().remove(ingest_id);

      if (OB_SUCCESS != ret) {
        const std::string detail_msg =
            flight_sql_ingest_error_message(ret, advertised_arrow_schema);
        return ob_sql_exec_ret_to_arrow_status(
            ret, proc.trace_id().c_str(),
            sql.substr(0, std::min(size_t(200), sql.size())), &detail_msg);
      }
      const std::string result = "{\"rows\":" + std::to_string(affected) + "}";
      return ob_make_ingest_result_stream("execute_ingest", result);

    } else if (action == "cancel_ingest") {
      const std::string ingest_id = ob_json_str(body_json, "ingest_id");
      if (!ingest_id.empty()) {
        (void)ObFlightSqlIngestRegistry::instance().abort(ingest_id);
        // Also abort the local ":stream" entry if this node was an assigned slot.
        (void)ObFlightSqlIngestRegistry::instance().abort(ingest_id + ":stream");
      }
      {
        lib::ObMallocHookAttrGuard malloc_guard(
            common::ObMemAttr(session.tenant_id, "FlightSQLSess"),
            false /* use_500 */);
        std::lock_guard<std::mutex> g(session.mutex);
        session.last_ingest_id.clear();
      }
      return ob_make_ingest_result_stream("cancel_ingest", "{}");

    } else {
      return arrow::Status::NotImplemented(
          "[FlightSQL] unknown ingest action: " + action);
    }
  }

  // ============================================================
  // State
  // ============================================================

  ObFlightSqlSessionManager session_mgr_;
  ObFlightSqlRpcCancelRegistry rpc_cancel_registry_;
  share::ObGlobalContext *gctx_;

  std::atomic<uint64_t> query_seq_{0};
  std::string next_query_id()
  {
    const uint64_t n = query_seq_.fetch_add(1, std::memory_order_relaxed);
    return "q" + std::to_string(n);
  }
};

// ============================================================
// ObOceanBaseFlightSqlServer (thin wrapper, pimpl)
// ============================================================

ObOceanBaseFlightSqlServer::ObOceanBaseFlightSqlServer(share::ObGlobalContext *gctx)
    : impl_(std::make_shared<ObOceanBaseFlightSqlServerImpl>(gctx))
{
  LOG_TRACE("[FlightSQL] ObOceanBaseFlightSqlServer created");
}

ObOceanBaseFlightSqlServer::~ObOceanBaseFlightSqlServer()
{
  LOG_TRACE("[FlightSQL] ObOceanBaseFlightSqlServer destroyed");
}

int ObOceanBaseFlightSqlServer::init_listener(const int32_t port)
{
  int ret = OB_SUCCESS;
  try {
  const bool tls_enabled = GCONF.flight_sql_tls_enabled;
  LOG_TRACE("[FlightSQL] init_listener", K(port), K(tls_enabled));

  const int max_threads = static_cast<int>(GCONF._flight_sql_worker_threads);
  const int min_pollers = static_cast<int>(GCONF._flight_sql_min_pollers);
  const int max_pollers = static_cast<int>(GCONF._flight_sql_max_pollers);

  LOG_TRACE("[FlightSQL] gRPC server options",
            K(max_threads), K(min_pollers), K(max_pollers), K(tls_enabled));

  if (min_pollers > max_pollers) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("[FlightSQL] _flight_sql_min_pollers must not exceed _flight_sql_max_pollers",
              K(ret), K(min_pollers), K(max_pollers));
    return ret;
  }

  if (!tls_enabled) {
    LOG_WARN("[FlightSQL] TLS is disabled; Basic credentials and bearer tokens are not "
             "transport-encrypted. Restrict the listener to a trusted network or "
             "TLS-terminating gateway",
             K(port));
  }

  common::ObArenaAllocator tls_allocator("FlightTLS");
  common::ObString tls_public_cert;
  common::ObString tls_private_key;
  DEFER(flight_sql_cleanse_ob_string(tls_private_key));
  if (tls_enabled
      && OB_FAIL(ObSrvNetworkFrame::load_flight_tls_credentials(tls_allocator,
                                                                tls_public_cert,
                                                                tls_private_key))) {
    LOG_ERROR("[FlightSQL] failed to load TLS credentials", K(ret));
    return ret;
  }

  arrow::Result<arrow::flight::Location> loc_res =
      tls_enabled
          ? arrow::flight::Location::ForGrpcTls("0.0.0.0", port)
          : arrow::flight::Location::ForGrpcTcp("0.0.0.0", port);
  if (!loc_res.ok()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[FlightSQL] failed to create listener location", K(ret), K(tls_enabled),
              "arrow_st", loc_res.status().ToString().c_str());
    return ret;
  }

  arrow::flight::FlightServerOptions options(loc_res.ValueOrDie());
  options.auth_handler = std::make_shared<ObFlightSqlAuthHandler>(&impl_->session_mgr_);
  DEFER(if (!options.tls_certificates.empty()) {
    flight_sql_cleanse_string(options.tls_certificates[0].pem_key);
  });
  if (tls_enabled) {
    options.tls_certificates.emplace_back();
    options.tls_certificates.back().pem_cert.assign(
        tls_public_cert.ptr(), static_cast<size_t>(tls_public_cert.length()));
    options.tls_certificates.back().pem_key.assign(
        tls_private_key.ptr(), static_cast<size_t>(tls_private_key.length()));
    options.verify_client = false;
  }
  // ResourceQuota::SetMaxThreads() is applied only when _flight_sql_worker_threads > 0.
  // 0 (default) means unlimited: gRPC manages threads freely up to MAX_POLLERS.
  // Setting this too low relative to MAX_POLLERS causes RESOURCE_EXHAUSTED because
  // ResourceQuota caps gRPC's total thread creation (pollers + infrastructure combined).
  ObFlightSqlBuilderHook hook = {max_threads, min_pollers, max_pollers,
                                  &impl_->rpc_cancel_registry_};
  options.builder_hook = hook;

  const arrow::Status st = impl_->Init(options);
  if (!st.ok()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[FlightSQL] Flight server Init failed", K(ret), "arrow_st", st.ToString().c_str());
  } else {
    LOG_TRACE("[FlightSQL] Flight server initialized", K(port), K(tls_enabled));
  }
  return ret;
  } OB_FLIGHT_SQL_CATCH_INIT_LISTENER
}

int ObOceanBaseFlightSqlServer::run_serve()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("[FlightSQL] run_serve called");
  const arrow::Status st = impl_->Serve();
  if (!st.ok()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[FlightSQL] Serve returned error", K(ret), "arrow_st", st.ToString().c_str());
  }
  return ret;
}

int ObOceanBaseFlightSqlServer::shutdown_server()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("[FlightSQL] shutdown_server called");
  const arrow::Status st = impl_->Shutdown();
  if (!st.ok()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] Shutdown failed", K(ret), "arrow_st", st.ToString().c_str());
  }
  return ret;
}

int ObOceanBaseFlightSqlServer::listener_port() const
{
  return impl_->port();
}

} // namespace observer
} // namespace oceanbase

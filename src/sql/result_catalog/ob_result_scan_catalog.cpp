/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/result_catalog/ob_result_scan_catalog.h"

#include <algorithm>

#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h"
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_sort.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/catalog/ob_catalog_properties.h"
#include "share/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/parse_node.h"
#include "sql/result_catalog/ob_async_result_arrow_codec.h"
#include "sql/result_catalog/ob_async_result_metadata.h"
#include "sql/result_catalog/ob_result_file_pruner.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

namespace
{

static const int64_t MAX_RESULT_METADATA_LENGTH = 64L * 1024 * 1024;
static const char *const RESULT_SCAN_METADATA_FILE_NAME = "metadata.json";

struct ObAsyncQueryResultLifecycle
{
  ObAsyncQueryResultLifecycle()
      : is_finished_(false), is_successful_(false), completion_time_us_(0) {}

  TO_STRING_KV(K_(is_finished), K_(is_successful), K_(completion_time_us));

  bool is_finished_;
  bool is_successful_;
  int64_t completion_time_us_;
};

int report_result_scan_table_not_exist(const ObString &query_id)
{
  int ret = OB_TABLE_NOT_EXIST;
  ObCStringHelper helper;
  const char *query_id_str = helper.convert(query_id);
  if (OB_ISNULL(query_id_str)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to convert RESULT_SCAN query id", KR(ret), K(query_id));
  } else {
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, "RESULT_SCAN", query_id_str);
  }
  return ret;
}

int load_result_table_metadata(const uint64_t tenant_id,
                               const uint64_t job_id,
                               ObIAllocator &allocator,
                               ObResultTableMetadata &metadata,
                               ObString &result_root,
                               ObString &access_info)
{
  int ret = OB_SUCCESS;
  ObSqlString metadata_relative_path;
  share::ObBackupDest storage_dest;
  ObString metadata_path;
  int64_t metadata_length = 0;
  int64_t read_size = 0;
  char *json_buf = nullptr;
  char access_info_buf[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
  if (OB_FAIL(ObAsyncResultStorageUtils::build_result_metadata_relative_path(
          tenant_id, job_id, metadata_relative_path))) {
    LOG_WARN("failed to build async result metadata path", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(ObAsyncResultStorageUtils::resolve_internal_path(
                 allocator,
                 metadata_relative_path.string(),
                 storage_dest,
                 metadata_path))) {
    LOG_WARN("failed to resolve async result metadata path", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_ISNULL(storage_dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result storage destination has no access information", KR(ret));
  } else if (OB_FAIL(ObBackupIoAdapter::get_file_length(
                 metadata_path, storage_dest.get_storage_info(), metadata_length))) {
    LOG_WARN("failed to get async result metadata size", KR(ret), K(metadata_path));
  } else if (metadata_length <= 0 || metadata_length > MAX_RESULT_METADATA_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result metadata size", KR(ret), K(metadata_length), K(metadata_path));
  } else if (OB_ISNULL(json_buf = static_cast<char *>(allocator.alloc(metadata_length + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate async result metadata buffer", KR(ret), K(metadata_length));
  } else if (OB_FAIL(ObBackupIoAdapter::read_single_file(
                 metadata_path,
                 storage_dest.get_storage_info(),
                 json_buf,
                 metadata_length,
                 read_size,
                 ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN("failed to read async result metadata", KR(ret), K(metadata_path));
  } else if (read_size != metadata_length) {
    ret = OB_IO_ERROR;
    LOG_WARN("async result metadata was only partially read",
             KR(ret), K(metadata_path), K(metadata_length), K(read_size));
  } else if (FALSE_IT(json_buf[metadata_length] = '\0')) {
  } else if (OB_FAIL(metadata.from_json(ObString(metadata_length, json_buf)))) {
    LOG_WARN("failed to parse async result metadata", KR(ret), K(metadata_path));
  } else if (OB_FAIL(storage_dest.get_storage_info()->get_storage_info_str(
                 access_info_buf, sizeof(access_info_buf)))) {
    LOG_WARN("failed to serialize async result storage access information", KR(ret));
  } else {
    const int64_t file_name_length = STRLEN(RESULT_SCAN_METADATA_FILE_NAME);
    if (!metadata_path.suffix_match(RESULT_SCAN_METADATA_FILE_NAME)
        || metadata_path.length() <= file_name_length) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid resolved async result metadata path", KR(ret), K(metadata_path));
    } else {
      const ObString root_view(metadata_path.length() - file_name_length, metadata_path.ptr());
      if (OB_FAIL(ob_write_string(allocator, root_view, result_root, true))) {
        LOG_WARN("failed to copy async result root", KR(ret), K(root_view));
      } else if (OB_FAIL(ob_write_string(
                     allocator, ObString::make_string(access_info_buf), access_info, true))) {
        LOG_WARN("failed to copy async result access information", KR(ret));
      }
    }
  }
  return ret;
}

int load_result_catalog_table_metadata(const uint64_t tenant_id,
                                       const uint64_t job_id,
                                       ObIAllocator &allocator,
                                       ObResultCatalogTableMetadata &catalog_metadata)
{
  int ret = OB_SUCCESS;
  ObString result_root;
  ObString access_info;
  catalog_metadata.set_identity(tenant_id, job_id);
  if (OB_FAIL(load_result_table_metadata(
          tenant_id,
          job_id,
          allocator,
          catalog_metadata.get_result_table_metadata(),
          result_root,
          access_info))) {
    if (OB_NOT_SUPPORTED == ret) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "using external async result store without shared storage mode is");
    }
    LOG_WARN("failed to load Result Catalog metadata", KR(ret), K(tenant_id), K(job_id));
  } else {
    catalog_metadata.set_storage_info(result_root, access_info);
  }
  return ret;
}

int set_result_column_properties(const ObResultColumnMetadata &metadata,
                                 const uint64_t table_id,
                                 ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  const ObObjType ob_type = static_cast<ObObjType>(metadata.ob_type_id_);
  const ObCollationType collation = static_cast<ObCollationType>(metadata.collation_id_);
  const char *expected_type_name = ob_obj_type_str(ob_type);
  if (metadata.ob_type_id_ < ObNullType || metadata.ob_type_id_ >= ObMaxType
      || metadata.field_id_ <= 0 || metadata.ordinal_ < 0
      || metadata.precision_ > INT16_MAX || metadata.scale_ > INT16_MAX
      || metadata.length_ > INT32_MAX || metadata.length_semantics_ > INT16_MAX
      || metadata.collation_id_ < CS_TYPE_INVALID || metadata.collation_id_ >= CS_TYPE_MAX
      || OB_ISNULL(expected_type_name)
      || 0 != metadata.ob_type_name_.case_compare(expected_type_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid OB type information in async result metadata", KR(ret), K(metadata));
  } else if (!ObAsyncResultArrowCodec::is_supported_result_column_metadata(metadata)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported async result Arrow to OB mapping", KR(ret), K(metadata));
  } else {
    column.set_tenant_id(OB_INVALID_TENANT_ID);
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID + metadata.ordinal_);
    column.set_data_type(ob_type);
    column.set_nullable(metadata.nullable_);
    column.set_collation_type(collation);
    column.set_charset_type(ObCharset::charset_type_by_coll(collation));
    if (metadata.precision_ >= 0) {
      column.set_data_precision(static_cast<int16_t>(metadata.precision_));
    }
    if (metadata.scale_ >= 0) {
      column.set_data_scale(static_cast<int16_t>(metadata.scale_));
    }
    if (metadata.length_ >= 0) {
      column.set_data_length(static_cast<int32_t>(metadata.length_));
    }
    if (metadata.length_semantics_ >= 0) {
      column.set_length_semantics(static_cast<int16_t>(metadata.length_semantics_));
    }
    if (OB_FAIL(column.set_column_name(metadata.name_))) {
      LOG_WARN("failed to set async result column name", KR(ret), K(metadata.name_));
    } else if ((ObCollectionSQLType == ob_type || ObSetType == ob_type)
               && OB_FAIL(column.set_extended_type_info(
                      metadata.extended_type_info_))) {
      LOG_WARN("failed to set async result extended type descriptor",
               KR(ret), K(metadata.extended_type_info_));
    }
  }
  return ret;
}

int add_result_hidden_columns(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t column_ids[] = {
      OB_HIDDEN_FILE_ID_COLUMN_ID, OB_HIDDEN_LINE_NUMBER_COLUMN_ID};
  const char *column_names[] = {
      OB_HIDDEN_FILE_ID_COLUMN_NAME, OB_HIDDEN_LINE_NUMBER_COLUMN_NAME};
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(column_ids); ++i) {
    ObColumnSchemaV2 column;
    column.set_tenant_id(table_schema.get_tenant_id());
    column.set_table_id(table_schema.get_table_id());
    column.set_column_id(column_ids[i]);
    column.set_data_type(ObIntType);
    column.set_nullable(false);
    column.set_is_hidden(true);
    column.set_charset_type(CHARSET_BINARY);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_rowkey_position(i + 1);
    if (OB_FAIL(column.set_column_name(column_names[i]))) {
      LOG_WARN("failed to set async result hidden column name", KR(ret), K(i));
    } else if (OB_FAIL(table_schema.add_column(column))) {
      LOG_WARN("failed to add async result hidden column", KR(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    table_schema.set_table_pk_exists_mode(ObTablePrimaryKeyExistsMode::TOM_TABLE_WITHOUT_PK);
  }
  return ret;
}

int build_parquet_result_table_schema(const uint64_t tenant_id,
                                      const uint64_t database_id,
                                      const uint64_t job_id,
                                      const ObResultCatalogTableMetadata &catalog_metadata,
                                      ObIAllocator &allocator,
                                      ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  const ObResultTableMetadata &metadata = catalog_metadata.get_result_table_metadata();
  const ObString &result_root = catalog_metadata.get_result_root();
  const ObString &access_info = catalog_metadata.get_access_info();
  ObExternalFileFormat format;
  ObString format_string;
  ObSqlString table_name;
  const uint64_t template_table_id = OB_MIN_EXTERNAL_OBJECT_ID + 1;
  table_schema = nullptr;
  format.format_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  // Position mapping is deliberate: SQL result schemas may contain quoted or
  // duplicate labels, while the private codec is defined by stable ordinal.
  format.parquet_format_.column_index_type_ = ColumnIndexType::POSITION;
  format.parquet_format_.column_name_case_sensitive_ = true;
  if (OB_UNLIKELY(!catalog_metadata.matches(tenant_id, job_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Result Catalog metadata identity does not match result scan",
             KR(ret), K(tenant_id), K(job_id), K(catalog_metadata));
  } else if (OB_FAIL(metadata.validate())) {
    LOG_WARN("invalid published Result Catalog metadata", KR(ret), K(tenant_id), K(job_id));
  } else if (result_root.empty() || access_info.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("incomplete Result Catalog storage metadata", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(format.to_string_with_alloc(format_string, allocator))) {
    LOG_WARN("failed to serialize parquet result format", KR(ret));
  } else if (OB_FAIL(table_name.assign_fmt("__result_scan_%016lx",
                                           static_cast<unsigned long>(job_id)))) {
    LOG_WARN("failed to build parquet result table name", KR(ret), K(job_id));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, table_schema))) {
    LOG_WARN("failed to allocate parquet result table schema", KR(ret));
  } else {
    table_schema->set_tenant_id(tenant_id);
    table_schema->set_catalog_id(OB_INTERNAL_CATALOG_ID);
    table_schema->set_database_id(database_id);
    table_schema->set_table_id(template_table_id);
    table_schema->set_table_type(EXTERNAL_TABLE);
    table_schema->set_schema_version(1);
    table_schema->set_lake_table_format(share::ObLakeTableFormat::RESULT);
    table_schema->set_collation_type(CS_TYPE_UTF8MB4_BIN);
    table_schema->set_charset_type(CHARSET_UTF8MB4);
    if (OB_FAIL(table_schema->set_table_name(table_name.string()))) {
      LOG_WARN("failed to set parquet result table name", KR(ret), K(table_name));
    } else if (OB_FAIL(table_schema->set_external_file_format(format_string))) {
      LOG_WARN("failed to set parquet result file format", KR(ret));
    } else if (OB_FAIL(table_schema->set_external_file_location(result_root))) {
      LOG_WARN("failed to set parquet result location", KR(ret), K(result_root));
    } else if (OB_FAIL(table_schema->set_external_file_location_access_info(access_info))) {
      LOG_WARN("failed to set parquet result access information", KR(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < metadata.get_columns().count(); ++i) {
    ObColumnSchemaV2 column;
    if (OB_FAIL(set_result_column_properties(
            metadata.get_columns().at(i), template_table_id, column))) {
      LOG_WARN("failed to build parquet result column", KR(ret), K(i));
    } else {
      column.set_tenant_id(tenant_id);
      if (OB_FAIL(table_schema->add_column(column))) {
        LOG_WARN("failed to add parquet result column", KR(ret), K(i), K(column));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_result_hidden_columns(*table_schema))) {
    LOG_WARN("failed to add parquet result hidden columns", KR(ret));
  }
  return ret;
}

int rebind_parquet_result_table_schema(ObTableSchema &table_schema,
                                       const uint64_t tenant_id,
                                       const uint64_t database_id,
                                       const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_database_id(database_id);
  table_schema.set_table_id(table_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); ++i) {
    ObColumnSchemaV2 *column = table_schema.get_column_schema_by_idx(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null column in parquet result schema", KR(ret), K(i));
    } else {
      column->set_tenant_id(tenant_id);
      column->set_table_id(table_id);
    }
  }
  return ret;
}

int extract_result_scan_show_args(const ParseNode *table_collection_expr,
                                  ObString &query_id,
                                  int64_t &requested_chunk_no)
{
  int ret = OB_SUCCESS;
  query_id.reset();
  requested_chunk_no = OB_INVALID_INDEX_INT64;
  const ParseNode *func_expr = nullptr;
  const ParseNode *func_name = nullptr;
  const ParseNode *params = nullptr;
  const ParseNode *query_id_node = nullptr;
  const ParseNode *chunk_no_node = nullptr;
  if (OB_ISNULL(table_collection_expr)
      || T_TABLE_COLLECTION_EXPRESSION != table_collection_expr->type_
      || 2 != table_collection_expr->num_child_
      || OB_ISNULL(table_collection_expr->children_)
      || OB_ISNULL(func_expr = table_collection_expr->children_[0])
      || OB_NOT_NULL(table_collection_expr->children_[1])) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DESC TABLE expression");
    LOG_WARN("unsupported DESC table collection expression", KR(ret),
             KP(table_collection_expr));
  } else if (T_FUN_SYS != func_expr->type_
             || 2 != func_expr->num_child_
             || OB_ISNULL(func_expr->children_)
             || OB_ISNULL(func_name = func_expr->children_[0])
             || OB_ISNULL(params = func_expr->children_[1])
             || OB_ISNULL(func_name->str_value_)
             || 0 != ObString(static_cast<int32_t>(func_name->str_len_),
                               func_name->str_value_).case_compare("result_scan")) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DESC TABLE expression other than RESULT_SCAN");
    LOG_WARN("DESC TABLE requires RESULT_SCAN", KR(ret), KP(func_expr));
  } else if (T_EXPR_LIST != params->type_
             || (1 != params->num_child_ && 2 != params->num_child_)
             || OB_ISNULL(params->children_)
             || OB_ISNULL(query_id_node = params->children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "RESULT_SCAN query_id must be a literal string");
    LOG_WARN("invalid RESULT_SCAN arguments in DESC", KR(ret), KP(params));
  } else if ((T_VARCHAR != query_id_node->type_ && T_CHAR != query_id_node->type_)
             || OB_ISNULL(query_id_node->str_value_)
             || query_id_node->str_len_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "RESULT_SCAN query_id must be a literal string");
    LOG_WARN("RESULT_SCAN query_id must be a literal string in DESC", KR(ret),
             KP(query_id_node));
  } else {
    query_id.assign_ptr(query_id_node->str_value_,
                        static_cast<int32_t>(query_id_node->str_len_));
    if (2 == params->num_child_) {
      chunk_no_node = params->children_[1];
      if (OB_ISNULL(chunk_no_node) || T_INT != chunk_no_node->type_
          || chunk_no_node->value_ < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "RESULT_SCAN chunk_id must be a non-negative literal integer");
        LOG_WARN("RESULT_SCAN chunk_id must be a literal integer in DESC", KR(ret),
                 KP(chunk_no_node));
      } else {
        requested_chunk_no = chunk_no_node->value_;
      }
    }
  }
  return ret;
}

int append_show_sql_string_literal(ObSqlString &sql, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (value.empty()) {
    ret = sql.append("''");
  } else if (OB_FAIL(sql.append("CONVERT(0x"))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
      ret = sql.append_fmt("%02X", static_cast<unsigned char>(value.ptr()[i]));
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append(" USING utf8mb4)"))) {
      LOG_WARN("failed to append Result Catalog show string literal", KR(ret));
    }
  }
  return ret;
}

int get_result_column_type(const ObColumnSchemaV2 &column,
                           const ObSQLSessionInfo *session,
                           ObIAllocator &allocator,
                           ObString &type)
{
  int ret = OB_SUCCESS;
  char stack_buf[OB_MAX_SYS_PARAM_NAME_LENGTH] = {0};
  char *type_buf = stack_buf;
  int64_t type_buf_len = sizeof(stack_buf);
  int64_t type_pos = 0;
  type.reset();
  const uint64_t sub_type = column.is_xmltype()
      ? column.get_sub_data_type() : static_cast<uint64_t>(column.get_geo_type());
  const ObLengthSemantics default_length_semantics = OB_NOT_NULL(session)
      ? session->get_actual_nls_length_semantics() : LS_BYTE;
  if (OB_FAIL(ob_sql_type_str(column.get_meta_type(),
                              column.get_accuracy(),
                              column.get_extended_type_info(),
                              default_length_semantics,
                              type_buf,
                              type_buf_len,
                              type_pos,
                              sub_type,
                              column.is_string_lob()))) {
    if (OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("failed to format Result Catalog column type", KR(ret), K(column));
    } else if (OB_ISNULL(type_buf = static_cast<char *>(
                   allocator.alloc(OB_MAX_EXTENDED_TYPE_INFO_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate Result Catalog column type buffer", KR(ret));
    } else {
      type_buf_len = OB_MAX_EXTENDED_TYPE_INFO_LENGTH;
      type_pos = 0;
      ret = ob_sql_type_str(column.get_meta_type(),
                            column.get_accuracy(),
                            column.get_extended_type_info(),
                            default_length_semantics,
                            type_buf,
                            type_buf_len,
                            type_pos,
                            sub_type,
                            column.is_string_lob());
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to extend Result Catalog column type buffer", KR(ret), K(column));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator,
                          ObString(static_cast<int32_t>(type_pos), type_buf),
                          type);
  }
  return ret;
}

int append_result_show_column_type(ObSqlString &sql,
                                   const ObColumnSchemaV2 &column,
                                   const ObSQLSessionInfo *session,
                                   ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString type;
  if (OB_FAIL(get_result_column_type(column, session, allocator, type))) {
    LOG_WARN("failed to format Result Catalog column type", KR(ret), K(column));
  } else if (OB_FAIL(append_show_sql_string_literal(sql, type))) {
    LOG_WARN("failed to append Result Catalog SHOW column type", KR(ret), K(column));
  }
  return ret;
}

int append_result_show_column_row(ObSqlString &sql,
                                  const ObColumnSchemaV2 &column,
                                  const ObSQLSessionInfo *session,
                                  ObIAllocator &allocator,
                                  const bool is_full,
                                  const bool is_first)
{
  int ret = OB_SUCCESS;
  const char *collation_name = ObCharset::collation_name(column.get_collation_type());
  const ObString nullable = column.is_nullable()
      ? ObString::make_string("YES") : ObString::make_string("NO");
  if (!is_first && OB_FAIL(sql.append(" UNION ALL "))) {
  } else if (OB_FAIL(sql.append("SELECT "))) {
  } else if (OB_FAIL(append_show_sql_string_literal(sql, column.get_column_name_str()))) {
  } else if (OB_FAIL(sql.append(" AS `Field`, "))) {
  } else if (OB_FAIL(append_result_show_column_type(sql, column, session, allocator))) {
  } else if (OB_FAIL(sql.append(" AS `Type`, "))) {
  } else if (is_full && OB_FAIL(append_show_sql_string_literal(
                 sql, ObString::make_string(OB_NOT_NULL(collation_name) ? collation_name : "")))) {
  } else if (is_full && OB_FAIL(sql.append(" AS `Collation`, "))) {
  } else if (OB_FAIL(append_show_sql_string_literal(sql, nullable))) {
  } else if (OB_FAIL(sql.append(" AS `Null`, '' AS `Key`, NULL AS `Default`, '' AS `Extra`"))) {
  } else if (is_full && OB_FAIL(sql.append(", '' AS `Privileges`, "))) {
  } else if (is_full && OB_FAIL(append_show_sql_string_literal(sql, column.get_comment_str()))) {
  } else if (is_full && OB_FAIL(sql.append(" AS `Comment`"))) {
  }
  return ret;
}

int append_result_identifier(ObSqlString &sql, const ObString &identifier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("`"))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < identifier.length(); ++i) {
      if ('`' == identifier[i]) {
        ret = sql.append("``");
      } else {
        ret = sql.append_fmt("%c", identifier[i]);
      }
    }
    if (OB_SUCC(ret)) {
      ret = sql.append("`");
    }
  }
  return ret;
}

int append_result_sql_literal(ObSqlString &sql, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("'"))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
      if ('\'' == value[i]) {
        ret = sql.append("''");
      } else {
        ret = sql.append_fmt("%c", value[i]);
      }
    }
    if (OB_SUCC(ret)) {
      ret = sql.append("'");
    }
  }
  return ret;
}

int append_result_create_column_def(ObSqlString &sql,
                                    const ObColumnSchemaV2 &column,
                                    const ObSQLSessionInfo *session,
                                    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString type;
  if (OB_FAIL(append_result_identifier(sql, column.get_column_name_str()))) {
  } else if (OB_FAIL(sql.append(" "))) {
  } else if (OB_FAIL(get_result_column_type(column, session, allocator, type))) {
    LOG_WARN("failed to format Result Catalog CREATE column type", KR(ret), K(column));
  } else if (OB_FAIL(sql.append(type))) {
  } else if (!column.is_nullable() && OB_FAIL(sql.append(" NOT NULL"))) {
  }
  return ret;
}

int build_external_result_files_json(const ObResultTableMetadata &manifest,
                                     const bool has_chunk_count,
                                     ObSqlString &files_json)
{
  int ret = OB_SUCCESS;
  bool is_first_file = true;
  int64_t file_ordinal = 0;
  if (OB_FAIL(files_json.append("["))) {
  }
  for (int64_t partition_idx = 0;
       OB_SUCC(ret) && partition_idx < manifest.get_partitions().count(); ++partition_idx) {
    const ObResultPartitionMetadata &partition = manifest.get_partitions().at(partition_idx);
    for (int64_t file_idx = 0;
         OB_SUCC(ret) && file_idx < partition.files_.count(); ++file_idx) {
      const ObResultFileMetadata &file = partition.files_.at(file_idx);
      if (!is_first_file && OB_FAIL(files_json.append(","))) {
      } else if (OB_FAIL(files_json.append_fmt(
                     "{\"file_ordinal\":%ld,\"partition_id\":%ld,\"row_count\":%ld,\"byte_count\":%ld",
                     file_ordinal,
                     partition.partition_id_,
                     file.row_count_,
                     file.byte_count_))) {
      } else if (has_chunk_count && OB_FAIL(files_json.append_fmt(
                     ",\"chunk_count\":%ld", file.row_groups_.count()))) {
      } else if (OB_FAIL(files_json.append("}"))) {
      } else {
        is_first_file = false;
        ++file_ordinal;
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(files_json.append("]"))) {
  }
  return ret;
}

int build_external_result_ordering(const ObResultTableMetadata &manifest,
                                   ObSqlString &ordering)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObResultColumnMetadata> &columns = manifest.get_columns();
  const ObIArray<ObResultOrderingFieldMetadata> &ordering_fields =
      manifest.get_ordering_fields();
  // SHOW CREATE keeps its existing file-level wording. The private manifest
  // carries the stronger writer-stream contract used by Result readers.
  const ObString public_mode = manifest.has_writer_file_ordering()
      ? ObString::make_string("SORTED_WITHIN_FILE") : manifest.get_ordering_mode();
  if (OB_FAIL(ordering.append(public_mode))) {
  } else if (!ordering_fields.empty() && OB_FAIL(ordering.append(" ("))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ordering_fields.count(); ++i) {
    const ObResultOrderingFieldMetadata &ordering_field = ordering_fields.at(i);
    const ObResultColumnMetadata *column = nullptr;
    for (int64_t j = 0; OB_ISNULL(column) && j < columns.count(); ++j) {
      if (ordering_field.field_id_ == columns.at(j).field_id_) {
        column = &columns.at(j);
      }
    }
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Result Catalog SHOW CREATE ordering field is absent from schema",
               KR(ret), K(i), K(ordering_field));
    } else if (i > 0 && OB_FAIL(ordering.append(", "))) {
    } else if (OB_FAIL(append_result_identifier(ordering, column->name_))) {
    } else if (OB_FAIL(ordering.append(ordering_field.ascending_ ? " ASC" : " DESC"))) {
    } else if (OB_FAIL(ordering.append(
                   ordering_field.nulls_first_ ? " NULLS FIRST" : " NULLS LAST"))) {
    }
  }
  if (OB_SUCC(ret) && !ordering_fields.empty() && OB_FAIL(ordering.append(")"))) {
  }
  return ret;
}

int build_external_result_create_sql(const ObString &query_id,
                                     const ObResultScanTableDesc &result_table,
                                     const ObSQLSessionInfo *session,
                                     ObIAllocator &allocator,
                                     ObString &table_name,
                                     ObString &create_sql)
{
  int ret = OB_SUCCESS;
  const ObResultCatalogTableMetadata *catalog_metadata = nullptr;
  const ObResultTableMetadata *manifest = nullptr;
  const ObTableSchema *table_schema = result_table.table_schema_;
  ObSqlString generated_table_name;
  ObSqlString generated_create_sql;
  ObSqlString files_json;
  ObSqlString ordering;
  int64_t chunk_count = 0;
  bool has_chunk_count = false;
  bool is_first_column = true;
  if (OB_ISNULL(result_table.catalog_table_metadata_)
      || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external Result Catalog table for SHOW CREATE", KR(ret),
             KP(result_table.catalog_table_metadata_), KP(table_schema));
  } else if (FALSE_IT(catalog_metadata = static_cast<const ObResultCatalogTableMetadata *>(
                          result_table.catalog_table_metadata_))) {
  } else if (FALSE_IT(manifest = &catalog_metadata->get_result_table_metadata())) {
  } else if (OB_FAIL(generated_table_name.append_fmt(
                 "result_scan_%016lx",
                 static_cast<unsigned long>(catalog_metadata->get_job_id())))) {
    LOG_WARN("failed to build external Result Catalog SHOW CREATE name", KR(ret), K(query_id));
  } else {
    const int chunk_ret = manifest->get_chunk_count(chunk_count);
    if (OB_SUCCESS == chunk_ret) {
      has_chunk_count = true;
    } else if (OB_NOT_SUPPORTED != chunk_ret) {
      ret = chunk_ret;
      LOG_WARN("failed to get external Result Catalog chunk count", KR(ret), K(query_id));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(build_external_result_ordering(*manifest, ordering))) {
    LOG_WARN("failed to build external Result Catalog ordering", KR(ret), K(query_id));
  } else if (OB_SUCC(ret) && OB_FAIL(build_external_result_files_json(
          *manifest, has_chunk_count, files_json))) {
    LOG_WARN("failed to build external Result Catalog file summary", KR(ret), K(query_id));
  } else if (OB_SUCC(ret) && OB_FAIL(generated_create_sql.append("CREATE EXTERNAL TABLE "))) {
  } else if (OB_SUCC(ret) && OB_FAIL(append_result_identifier(
                 generated_create_sql, generated_table_name.string()))) {
  } else if (OB_SUCC(ret) && OB_FAIL(generated_create_sql.append(" (\n"))) {
  }
  for (int64_t i = 0;
       OB_SUCC(ret) && i < table_schema->get_column_count(); ++i) {
    const ObColumnSchemaV2 *column = table_schema->get_column_schema_by_idx(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Result Catalog SHOW CREATE schema has null column", KR(ret), K(i), K(query_id));
    } else if (column->is_hidden()) {
      // Storage-only file/line columns are not part of the published result schema.
    } else if (!is_first_column && OB_FAIL(generated_create_sql.append(",\n"))) {
    } else if (OB_FAIL(generated_create_sql.append("  "))) {
    } else if (OB_FAIL(append_result_create_column_def(
                   generated_create_sql, *column, session, allocator))) {
      LOG_WARN("failed to append Result Catalog SHOW CREATE column", KR(ret), K(i), K(query_id));
    } else {
      is_first_column = false;
    }
  }
  if (OB_SUCC(ret) && is_first_column) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("external Result Catalog SHOW CREATE has no visible columns", KR(ret), K(query_id));
  } else if (OB_SUCC(ret) && OB_FAIL(generated_create_sql.append("\n)\nWITH (\n  result_store = 'EXTERNAL',\n  result_query_id = "))) {
  } else if (OB_SUCC(ret) && OB_FAIL(append_result_sql_literal(generated_create_sql, query_id))) {
  } else if (OB_SUCC(ret) && OB_FAIL(generated_create_sql.append_fmt(
                 ",\n  result_row_count = %ld,\n  result_file_count = %ld,\n  result_byte_count = %ld",
                 manifest->get_row_count(),
                 manifest->get_file_count(),
                 manifest->get_byte_count()))) {
  } else if (OB_SUCC(ret) && has_chunk_count && OB_FAIL(generated_create_sql.append_fmt(
                 ",\n  result_chunk_count = %ld", chunk_count))) {
  } else if (OB_SUCC(ret) && OB_FAIL(generated_create_sql.append(",\n  result_ordering = "))) {
  } else if (OB_SUCC(ret) && OB_FAIL(append_result_sql_literal(
                 generated_create_sql, ordering.string()))) {
  } else if (OB_SUCC(ret) && OB_FAIL(generated_create_sql.append(",\n  result_files = "))) {
  } else if (OB_SUCC(ret) && OB_FAIL(append_result_sql_literal(
                 generated_create_sql, files_json.string()))) {
  } else if (OB_SUCC(ret) && OB_FAIL(generated_create_sql.append("\n)"))) {
  } else if (OB_SUCC(ret) && OB_FAIL(ob_write_string(
                 allocator, generated_table_name.string(), table_name, true /* c_style */))) {
    LOG_WARN("failed to persist external Result Catalog SHOW CREATE name", KR(ret), K(query_id));
  } else if (OB_SUCC(ret) && OB_FAIL(ob_write_string(
                 allocator, generated_create_sql.string(), create_sql, true /* c_style */))) {
    LOG_WARN("failed to persist external Result Catalog SHOW CREATE SQL", KR(ret), K(query_id));
  }
  return ret;
}

bool is_async_query_terminal_failure(
    const dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  return 0 == job_info.state_.case_compare("FAILED")
      || 0 == job_info.state_.case_compare("BROKEN")
      || 0 == job_info.state_.case_compare("KILLED");
}

int get_async_query_result_lifecycle(
    const dbms_scheduler::ObDBMSSchedJobInfo &job_info,
    ObAsyncQueryResultLifecycle &lifecycle)
{
  int ret = OB_SUCCESS;
  const bool is_completed = 0 == job_info.state_.case_compare("COMPLETED");
  lifecycle = ObAsyncQueryResultLifecycle();
  lifecycle.is_finished_ = is_completed || is_async_query_terminal_failure(job_info);
  // Async-query jobs are one-shot. On finish, the scheduler atomically stores
  // last_date as the execution start and total as its elapsed runtime together
  // with state and failures, so their sum survives run-detail purging.
  if (is_completed
      && 0 == job_info.failures_
      && job_info.last_date_ > 0
      && job_info.total_ >= 0) {
    if (OB_UNLIKELY(job_info.total_ > INT64_MAX - job_info.last_date_)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("async query completion time overflows",
               KR(ret), K(job_info.last_date_), K(job_info.total_), K(job_info));
    } else {
      lifecycle.is_successful_ = true;
      lifecycle.completion_time_us_ = job_info.last_date_ + job_info.total_;
    }
  }
  return ret;
}

bool is_async_query_result_expired(const int64_t completion_time,
                                   const int64_t retention_us,
                                   const int64_t now)
{
  return completion_time <= 0
      || (now >= completion_time && now - completion_time >= retention_us);
}

int check_async_query_result_not_expired(
    const uint64_t tenant_id,
    const uint64_t job_id,
    const ObString &query_id,
    const int64_t completion_time_us,
    const ObResultTableMetadata *metadata)
{
  int ret = OB_SUCCESS;
  int64_t retention_us = 0;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t expire_time_us = OB_NOT_NULL(metadata) ? metadata->get_expire_time_us() : 0;
  bool expired = false;
  if (expire_time_us > 0) {
    expired = now >= expire_time_us;
  } else if (OB_FAIL(ObAsyncResultStorageUtils::get_result_retention_us(
                 tenant_id, retention_us))) {
    LOG_WARN("failed to get legacy async result retention time",
             KR(ret), K(tenant_id), K(job_id));
  } else {
    expired = is_async_query_result_expired(
        completion_time_us, retention_us, now);
  }
  if (OB_SUCC(ret) && expired) {
    ret = report_result_scan_table_not_exist(query_id);
    LOG_WARN("async query result has expired", KR(ret), K(tenant_id), K(job_id),
             K(completion_time_us), K(expire_time_us), K(retention_us), K(now));
  }
  return ret;
}

int get_async_query_jobs_in_tenant(const uint64_t tenant_id,
                                   ObMySQLProxy &sql_proxy,
                                   ObIAllocator &allocator,
                                   ObIArray<dbms_scheduler::ObDBMSSchedJobInfo> &jobs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  dbms_scheduler::ObDBMSSchedTableOperator table_operator;
  const uint64_t extract_tenant_id =
      ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  const uint64_t async_query_func_type = static_cast<uint64_t>(
      dbms_scheduler::ObDBMSSchedFuncType::ASYNC_QUERY_JOB);
  if (OB_FAIL(sql.assign_fmt(
          "SELECT * FROM %s WHERE tenant_id = %lu AND job > 0 AND func_type = %lu",
          share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
          static_cast<unsigned long>(extract_tenant_id),
          static_cast<unsigned long>(async_query_func_type)))) {
    LOG_WARN("failed to build async query cleanup lookup", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to list async query jobs for cleanup", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("async query cleanup lookup result is null", KR(ret), K(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          const int next_ret = result.get_result()->next();
          if (OB_ITER_END == next_ret) {
            break;
          } else if (OB_SUCCESS != next_ret) {
            ret = next_ret;
            LOG_WARN("failed to iterate async query cleanup lookup", KR(ret), K(tenant_id));
          } else {
            dbms_scheduler::ObDBMSSchedJobInfo job_info;
            if (OB_FAIL(table_operator.extract_info(
                    *(result.get_result()), tenant_id, false /* is_oracle_tenant */, allocator, job_info))) {
              LOG_WARN("failed to extract async query cleanup job", KR(ret), K(tenant_id));
            } else if (OB_UNLIKELY(!job_info.is_async_query_job())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("cleanup lookup returned a non-async-query job", KR(ret), K(job_info));
            } else if (OB_FAIL(jobs.push_back(job_info))) {
              LOG_WARN("failed to save async query cleanup job", KR(ret), K(job_info));
            }
          }
        }
      }
    }
  }
  return ret;
}

bool parse_lower_hex_id(const ObString &name,
                        const int64_t offset,
                        uint64_t &value)
{
  static const int64_t HEX_ID_LENGTH = 16;
  bool valid = offset >= 0 && name.length() >= offset + HEX_ID_LENGTH;
  value = 0;
  for (int64_t i = 0; valid && i < HEX_ID_LENGTH; ++i) {
    const char ch = name[offset + i];
    uint64_t digit = 0;
    if (ch >= '0' && ch <= '9') {
      digit = static_cast<uint64_t>(ch - '0');
    } else if (ch >= 'a' && ch <= 'f') {
      digit = static_cast<uint64_t>(ch - 'a' + 10);
    } else {
      valid = false;
    }
    if (valid) {
      value = (value << 4) | digit;
    }
  }
  return valid;
}

bool parse_external_result_job_dir(const ObString &dir_name, uint64_t &job_id)
{
  static const char *const JOB_DIR_PREFIX = "job=";
  static const int64_t JOB_DIR_PREFIX_LENGTH = 4;
  static const int64_t JOB_DIR_NAME_LENGTH = JOB_DIR_PREFIX_LENGTH + 16;
  return dir_name.length() == JOB_DIR_NAME_LENGTH
      && 0 == MEMCMP(dir_name.ptr(), JOB_DIR_PREFIX, JOB_DIR_PREFIX_LENGTH)
      && parse_lower_hex_id(dir_name, JOB_DIR_PREFIX_LENGTH, job_id)
      && job_id > 0
      && job_id != OB_INVALID_ID;
}

bool parse_native_result_table_name(const ObString &table_name,
                                    const uint64_t tenant_id,
                                    uint64_t &job_id)
{
  static const char *const TABLE_PREFIX = "__ob_aqr_t";
  static const int64_t TABLE_PREFIX_LENGTH = 10;
  static const int64_t TENANT_ID_OFFSET = TABLE_PREFIX_LENGTH;
  static const int64_t USER_MARKER_OFFSET = TENANT_ID_OFFSET + 16;
  static const int64_t USER_ID_OFFSET = USER_MARKER_OFFSET + 2;
  static const int64_t JOB_MARKER_OFFSET = USER_ID_OFFSET + 16;
  static const int64_t JOB_ID_OFFSET = JOB_MARKER_OFFSET + 2;
  static const int64_t TABLE_NAME_LENGTH = JOB_ID_OFFSET + 16;
  uint64_t parsed_tenant_id = OB_INVALID_ID;
  uint64_t user_id = OB_INVALID_ID;
  const bool valid = table_name.length() == TABLE_NAME_LENGTH
      && 0 == MEMCMP(table_name.ptr(), TABLE_PREFIX, TABLE_PREFIX_LENGTH)
      && table_name[USER_MARKER_OFFSET] == '_'
      && table_name[USER_MARKER_OFFSET + 1] == 'u'
      && table_name[JOB_MARKER_OFFSET] == '_'
      && table_name[JOB_MARKER_OFFSET + 1] == 'j'
      && parse_lower_hex_id(table_name, TENANT_ID_OFFSET, parsed_tenant_id)
      && parse_lower_hex_id(table_name, USER_ID_OFFSET, user_id)
      && parse_lower_hex_id(table_name, JOB_ID_OFFSET, job_id)
      && parsed_tenant_id == tenant_id
      && user_id != OB_INVALID_ID
      && job_id > 0
      && job_id != OB_INVALID_ID;
  return valid;
}

bool contains_job_id(const ObIArray<uint64_t> &job_ids, const uint64_t job_id)
{
  int64_t left = 0;
  int64_t right = job_ids.count();
  while (left < right) {
    const int64_t middle = left + (right - left) / 2;
    if (job_ids.at(middle) < job_id) {
      left = middle + 1;
    } else {
      right = middle;
    }
  }
  return left < job_ids.count() && job_ids.at(left) == job_id;
}

int check_async_query_job_exists(const uint64_t tenant_id,
                                 const uint64_t job_id,
                                 ObMySQLProxy &sql_proxy,
                                 bool &exists)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t extract_tenant_id =
      ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  const uint64_t async_query_func_type = static_cast<uint64_t>(
      dbms_scheduler::ObDBMSSchedFuncType::ASYNC_QUERY_JOB);
  exists = false;
  if (OB_FAIL(sql.assign_fmt(
          "SELECT 1 FROM %s WHERE tenant_id = %lu AND job = %lu "
          "AND func_type = %lu LIMIT 1",
          share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
          static_cast<unsigned long>(extract_tenant_id),
          static_cast<unsigned long>(job_id),
          static_cast<unsigned long>(async_query_func_type)))) {
    LOG_WARN("failed to build async query orphan recheck", KR(ret), K(tenant_id), K(job_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to recheck async query job", KR(ret), K(tenant_id), K(job_id));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("async query orphan recheck result is null", KR(ret), K(tenant_id), K(job_id));
      } else {
        const int next_ret = result.get_result()->next();
        if (OB_SUCCESS == next_ret) {
          exists = true;
        } else if (OB_ITER_END != next_ret) {
          ret = next_ret;
          LOG_WARN("failed to read async query orphan recheck", KR(ret), K(tenant_id), K(job_id));
        }
      }
    }
  }
  return ret;
}

int cleanup_native_async_query_result_table(const uint64_t tenant_id,
                                            const ObString &result_table_name,
                                            ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObString result_catalog_name =
      dbms_scheduler::ObDBMSSchedJobInfo::get_async_query_result_catalog_name();
  const ObString result_database_name =
      dbms_scheduler::ObDBMSSchedJobInfo::get_async_query_result_database_name();
  if (OB_UNLIKELY(result_table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty native async result cleanup table", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.append_fmt("DROP TABLE IF EXISTS `%.*s`.`%.*s`.`%.*s`",
                                   result_catalog_name.length(), result_catalog_name.ptr(),
                                   result_database_name.length(), result_database_name.ptr(),
                                   result_table_name.length(), result_table_name.ptr()))) {
    LOG_WARN("failed to build native async result cleanup SQL",
             KR(ret), K(tenant_id), K(result_table_name));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to drop native async result",
             KR(ret), K(tenant_id), K(result_table_name), K(sql));
  } else {
    LOG_DEBUG("cleaned native async result table",
             K(tenant_id), K(result_table_name), K(affected_rows));
  }
  return ret;
}

int cleanup_native_async_query_result(const uint64_t tenant_id,
                                      const dbms_scheduler::ObDBMSSchedJobInfo &job_info,
                                      ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString result_table_name;
  if (OB_FAIL(job_info.build_async_query_result_table_name(result_table_name))) {
    LOG_WARN("failed to build expired native async result table name", KR(ret), K(job_info));
  } else if (OB_FAIL(cleanup_native_async_query_result_table(
                 tenant_id, result_table_name.string(), sql_proxy))) {
    LOG_WARN("failed to clean native async result table", KR(ret), K(job_info));
  }
  return ret;
}

int get_native_async_result_table_names(const uint64_t tenant_id,
                                        ObMySQLProxy &sql_proxy,
                                        ObIAllocator &allocator,
                                        ObIArray<ObString> &table_names)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t extract_tenant_id =
      ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  const ObString result_database_name =
      dbms_scheduler::ObDBMSSchedJobInfo::get_async_query_result_database_name();
  if (OB_FAIL(sql.assign_fmt(
          "SELECT t.table_name FROM %s t JOIN %s d ON t.database_id = d.database_id "
          "WHERE t.tenant_id = %lu AND d.tenant_id = %lu AND t.session_id = 0 "
          "AND d.database_name = '%.*s'",
          share::OB_ALL_TABLE_TNAME,
          share::OB_ALL_DATABASE_TNAME,
          static_cast<unsigned long>(extract_tenant_id),
          static_cast<unsigned long>(extract_tenant_id),
          result_database_name.length(), result_database_name.ptr()))) {
    LOG_WARN("failed to build native async result table lookup", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to list native async result tables", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("native async result table lookup is null", KR(ret), K(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          const int next_ret = result.get_result()->next();
          ObString table_name;
          ObString stored_table_name;
          if (OB_ITER_END == next_ret) {
            break;
          } else if (OB_SUCCESS != next_ret) {
            ret = next_ret;
            LOG_WARN("failed to iterate native async result tables", KR(ret), K(tenant_id));
          } else if (OB_FAIL(result.get_result()->get_varchar("table_name", table_name))) {
            LOG_WARN("failed to read native async result table name", KR(ret), K(tenant_id));
          } else if (OB_FAIL(ob_write_string(allocator, table_name, stored_table_name, true))) {
            LOG_WARN("failed to save native async result table name", KR(ret), K(table_name));
          } else if (OB_FAIL(table_names.push_back(stored_table_name))) {
            LOG_WARN("failed to collect native async result table name", KR(ret), K(table_name));
          }
        }
      }
    }
  }
  return ret;
}

int cleanup_orphan_external_async_results(const uint64_t tenant_id,
                                          ObMySQLProxy &sql_proxy,
                                          const ObIArray<uint64_t> &known_job_ids)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ResultS3Orphan");
  share::ObBackupDest storage_dest;
  ObSqlString tenant_relative_path;
  ObString tenant_path;
  ObSEArray<ObString, 16> job_dirs;
  ObFileListArrayOp list_op(job_dirs, allocator);
  if (OB_FAIL(tenant_relative_path.assign_fmt(
          "tenant=%016lx/", static_cast<unsigned long>(tenant_id)))) {
    LOG_WARN("failed to build async result tenant path", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAsyncResultStorageUtils::resolve_internal_path(
                 allocator, tenant_relative_path.string(), storage_dest, tenant_path))) {
    if (OB_NOT_SUPPORTED == ret) {
      LOG_WARN("skip orphan external async result cleanup without shared storage",
               KR(ret), K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to resolve async result tenant path", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(storage_dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result orphan cleanup has no storage information", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObBackupIoAdapter::list_directories(
                 tenant_path, storage_dest.get_storage_info(), list_op))) {
    if (OB_FILE_NOT_EXIST == ret || OB_DIR_NOT_EXIST == ret
        || OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("async result tenant prefix is absent", K(tenant_id), K(tenant_path));
    } else {
      LOG_WARN("failed to list async result job prefixes", KR(ret), K(tenant_id), K(tenant_path));
    }
  }
  int cleanup_error = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < job_dirs.count(); ++i) {
    uint64_t job_id = OB_INVALID_ID;
    bool job_exists = false;
    if (!parse_external_result_job_dir(job_dirs.at(i), job_id)) {
      LOG_WARN("ignore unexpected directory in async Result Store",
               K(tenant_id), "directory", job_dirs.at(i));
    } else if (contains_job_id(known_job_ids, job_id)) {
      // The normal lifecycle pass owns this result.
    } else {
      const int check_ret = check_async_query_job_exists(
          tenant_id, job_id, sql_proxy, job_exists);
      if (OB_SUCCESS != check_ret) {
        if (OB_SUCCESS == cleanup_error) {
          cleanup_error = check_ret;
        }
        LOG_WARN("failed to recheck possible external async result orphan",
                 KR(check_ret), K(tenant_id), K(job_id));
      } else if (job_exists) {
        LOG_TRACE("keep async result created after cleanup snapshot", K(tenant_id), K(job_id));
      } else {
        const int cleanup_ret = ObAsyncResultStorageUtils::cleanup_result_job_prefix(
            tenant_id, job_id);
        if (OB_SUCCESS != cleanup_ret) {
          if (OB_SUCCESS == cleanup_error) {
            cleanup_error = cleanup_ret;
          }
          LOG_WARN("failed to clean orphan external async result",
                   KR(cleanup_ret), K(tenant_id), K(job_id));
        } else {
          LOG_DEBUG("cleaned orphan external async result", K(tenant_id), K(job_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = cleanup_error;
  }
  return ret;
}

int cleanup_orphan_native_async_results(const uint64_t tenant_id,
                                        ObMySQLProxy &sql_proxy,
                                        ObIAllocator &allocator,
                                        const ObIArray<uint64_t> &known_job_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 16> table_names;
  if (OB_FAIL(get_native_async_result_table_names(
          tenant_id, sql_proxy, allocator, table_names))) {
    LOG_WARN("failed to list native async result tables for orphan cleanup",
             KR(ret), K(tenant_id));
  }
  int cleanup_error = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_names.count(); ++i) {
    uint64_t job_id = OB_INVALID_ID;
    bool job_exists = false;
    if (!parse_native_result_table_name(table_names.at(i), tenant_id, job_id)) {
      LOG_WARN("ignore unexpected table in async result database",
               K(tenant_id), "table_name", table_names.at(i));
    } else if (contains_job_id(known_job_ids, job_id)) {
      // The normal lifecycle pass owns this result.
    } else {
      const int check_ret = check_async_query_job_exists(
          tenant_id, job_id, sql_proxy, job_exists);
      if (OB_SUCCESS != check_ret) {
        if (OB_SUCCESS == cleanup_error) {
          cleanup_error = check_ret;
        }
        LOG_WARN("failed to recheck possible native async result orphan",
                 KR(check_ret), K(tenant_id), K(job_id));
      } else if (job_exists) {
        LOG_TRACE("keep native result created after cleanup snapshot", K(tenant_id), K(job_id));
      } else {
        const int cleanup_ret = cleanup_native_async_query_result_table(
            tenant_id, table_names.at(i), sql_proxy);
        if (OB_SUCCESS != cleanup_ret) {
          if (OB_SUCCESS == cleanup_error) {
            cleanup_error = cleanup_ret;
          }
          LOG_WARN("failed to clean orphan native async result",
                   KR(cleanup_ret), K(tenant_id), K(job_id), "table_name", table_names.at(i));
        } else {
          LOG_DEBUG("cleaned orphan native async result",
                   K(tenant_id), K(job_id), "table_name", table_names.at(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = cleanup_error;
  }
  return ret;
}

int resolve_result_subschema(const ObColumnSchemaV2 &column_schema,
                             const ObSQLSessionInfo *session,
                             uint16_t &subschema_id)
{
  int ret = OB_SUCCESS;
  const bool is_collection = column_schema.is_collection();
  const bool is_enum_or_set = column_schema.is_enum_or_set();
  subschema_id = UINT_MAX16;
  if (!is_collection && !is_enum_or_set) {
    // no statement-local subschema metadata is required
  } else if (OB_UNLIKELY(column_schema.get_extended_type_info().empty())
             || (is_collection
                 && 1 != column_schema.get_extended_type_info().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid result column subschema metadata", KR(ret), K(column_schema));
  } else {
    ObExecContext *exec_ctx = OB_NOT_NULL(session)
        ? const_cast<ObSQLSessionInfo *>(session)->get_cur_exec_ctx() : nullptr;
    if (OB_ISNULL(exec_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no execution context for result column subschema", KR(ret));
    } else if (is_collection
               && OB_FAIL(exec_ctx->get_subschema_id_by_type_string(
                      column_schema.get_extended_type_info().at(0), subschema_id))) {
      LOG_WARN("failed to register collection result column subschema", KR(ret),
               "type_str", column_schema.get_extended_type_info().at(0));
    } else if (is_enum_or_set
               && OB_FAIL(exec_ctx->get_subschema_id_by_type_info(
                      column_schema.get_meta_type(),
                      column_schema.get_extended_type_info(),
                      subschema_id))) {
      LOG_WARN("failed to register ENUM/SET result column subschema", KR(ret));
    }
  }
  return ret;
}

int cleanup_one_async_query_result(const uint64_t tenant_id,
                                   dbms_scheduler::ObDBMSSchedJobInfo &job_info,
                                   const int64_t retention_us,
                                   ObMySQLProxy &sql_proxy,
                                   ObIAllocator &job_config_allocator)
{
  int ret = OB_SUCCESS;
  ObAsyncQueryResultLifecycle lifecycle;
  bool should_cleanup = false;
  dbms_scheduler::ObAsyncQueryJobConfig async_query_config;
  if (OB_FAIL(get_async_query_result_lifecycle(job_info, lifecycle))) {
    LOG_WARN("failed to derive async query result lifecycle", KR(ret), K(job_info));
  } else if (OB_FAIL(async_query_config.deserialize(
                 job_info.get_job_config(), job_config_allocator))) {
    LOG_WARN("failed to parse async query result configuration", KR(ret), K(job_info));
  } else if (lifecycle.is_finished_ && !lifecycle.is_successful_) {
    // Terminal jobs without a successful execution may still have partial
    // native/Parquet output. Their physical target is deterministic, so
    // remove it on the next daily pass.
    should_cleanup = true;
  }

  if (OB_SUCC(ret) && !should_cleanup && lifecycle.is_successful_) {
    if (dbms_scheduler::ObAsyncQueryResultStore::EXTERNAL
        == async_query_config.get_result_store()) {
      ObArenaAllocator metadata_allocator("ResultGcMeta");
      ObResultTableMetadata metadata(metadata_allocator);
      ObString result_root;
      ObString access_info;
      const int metadata_ret = load_result_table_metadata(
          tenant_id,
          job_info.get_job_id(),
          metadata_allocator,
          metadata,
          result_root,
          access_info);
      if (OB_NOT_SUPPORTED == metadata_ret) {
        LOG_WARN("skip external async result expiration check without shared storage",
                 KR(metadata_ret), K(job_info));
      } else if (OB_OBJECT_NOT_EXIST == metadata_ret
          || OB_FILE_NOT_EXIST == metadata_ret
          || OB_DIR_NOT_EXIST == metadata_ret
          || OB_ENTRY_NOT_EXIST == metadata_ret) {
        should_cleanup = true;
        LOG_DEBUG("completed external async result metadata is absent; clean remaining prefix",
                 KR(metadata_ret), K(job_info));
      } else if (OB_SUCCESS != metadata_ret) {
        ret = metadata_ret;
        LOG_WARN("failed to inspect external async result expiration",
                 KR(ret), K(job_info));
      } else if (metadata.get_expire_time_us() > 0) {
        should_cleanup = ObTimeUtility::current_time() >= metadata.get_expire_time_us();
      } else {
        // Compatibility for manifests published before expire_time_us was added.
        should_cleanup = is_async_query_result_expired(
            lifecycle.completion_time_us_, retention_us, ObTimeUtility::current_time());
      }
    } else {
      should_cleanup = is_async_query_result_expired(
          lifecycle.completion_time_us_, retention_us, ObTimeUtility::current_time());
    }
  }

  if (OB_SUCC(ret) && should_cleanup) {
    if (dbms_scheduler::ObAsyncQueryResultStore::EXTERNAL
        == async_query_config.get_result_store()) {
      if (OB_FAIL(ObAsyncResultStorageUtils::cleanup_result_job_prefix(
              tenant_id, job_info.get_job_id()))) {
        LOG_WARN("failed to clean expired external async result", KR(ret), K(job_info));
      }
    } else if (OB_FAIL(cleanup_native_async_query_result(tenant_id, job_info, sql_proxy))) {
      LOG_WARN("failed to clean expired native async result", KR(ret), K(job_info));
    }
    // Delete the active scheduler definition only after physical cleanup has
    // succeeded. Run-detail history remains independently available until its
    // normal purge, and an absent job is success for concurrent/retry cleanup.
    if (OB_SUCC(ret)
        && OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::remove_dbms_sched_job(
            sql_proxy,
            tenant_id,
            job_info.get_job_name(),
            true /* if_exists */))) {
      LOG_WARN("failed to remove cleaned async query scheduler job",
               KR(ret), K(tenant_id), K(job_info));
    } else if (OB_SUCC(ret)) {
      LOG_DEBUG("cleaned async query result and scheduler job",
                K(tenant_id), K(job_info), K(lifecycle), K(retention_us));
    }
  }
  return ret;
}

} // namespace

int ObResultScanCatalog::cleanup_expired_results(const uint64_t tenant_id,
                                                  ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ResultReaper");
  ObArenaAllocator job_config_allocator("ResultJobCfg");
  ObSEArray<dbms_scheduler::ObDBMSSchedJobInfo, 16> jobs;
  ObSEArray<uint64_t, 16> known_job_ids;
  int64_t retention_us = 0;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async result cleanup only applies to user tenants", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAsyncResultStorageUtils::get_result_retention_us(
                 tenant_id, retention_us))) {
    LOG_WARN("failed to get async result cleanup retention time", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_async_query_jobs_in_tenant(tenant_id, sql_proxy, allocator, jobs))) {
    LOG_WARN("failed to list async query results for cleanup", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
    if (OB_FAIL(known_job_ids.push_back(jobs.at(i).get_job_id()))) {
      LOG_WARN("failed to collect known async query job id", KR(ret), K(i), K(jobs.at(i)));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(known_job_ids.begin(), known_job_ids.end());
  }

  int cleanup_error = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
    job_config_allocator.reuse();
    const int job_ret = cleanup_one_async_query_result(
        tenant_id, jobs.at(i), retention_us, sql_proxy, job_config_allocator);
    if (OB_SUCCESS != job_ret) {
      if (OB_SUCCESS == cleanup_error) {
        cleanup_error = job_ret;
      }
      LOG_WARN("failed to clean one async query result; continue with remaining results",
               KR(job_ret), K(jobs.at(i)));
    }
  }
  if (OB_SUCC(ret)) {
    const int external_ret = cleanup_orphan_external_async_results(
        tenant_id, sql_proxy, known_job_ids);
    const int native_ret = cleanup_orphan_native_async_results(
        tenant_id, sql_proxy, allocator, known_job_ids);
    if (OB_SUCCESS != external_ret) {
      if (OB_SUCCESS == cleanup_error) {
        cleanup_error = external_ret;
      }
      LOG_WARN("failed to clean orphan external async results", KR(external_ret), K(tenant_id));
    }
    if (OB_SUCCESS != native_ret) {
      if (OB_SUCCESS == cleanup_error) {
        cleanup_error = native_ret;
      }
      LOG_WARN("failed to clean orphan native async results", KR(native_ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    ret = cleanup_error;
  }
  return ret;
}

int ObResultScanCatalog::get_result_table(const ObString &query_id,
                                          const ObSQLSessionInfo *session,
                                          ObSqlSchemaGuard *sql_schema_guard,
                                          ObIAllocator &alloc,
                                          ObResultScanTableDesc &table_desc,
                                          ObIArray<ObResultScanColDesc> &col_descs,
                                          const share::ObILakeTableMetadata *cached_metadata,
                                          const int64_t requested_chunk_no)
{
  int ret = OB_SUCCESS;
  table_desc.table_id_ = OB_INVALID_ID;
  table_desc.catalog_table_metadata_ = nullptr;
  table_desc.table_schema_ = nullptr;
  const uint64_t tenant_id = OB_NOT_NULL(session)
      ? session->get_effective_tenant_id() : OB_INVALID_TENANT_ID;
  const uint64_t current_user_id = OB_NOT_NULL(session)
      ? session->get_priv_user_id() : OB_INVALID_ID;
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *result_table_schema = nullptr;
  ObTableSchema *allocated_result_table_schema = nullptr;
  uint64_t result_database_id = OB_INVALID_ID;
  ObSqlString result_table_name;
  ObArenaAllocator job_config_allocator("ResultJobCfg");
  ObAsyncQueryResultLifecycle lifecycle;
  dbms_scheduler::ObAsyncQueryJobConfig async_query_config;

  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result_scan session is null", KR(ret));
  } else if (OB_UNLIKELY(query_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "RESULT_SCAN query_id");
    LOG_WARN("invalid result_scan query_id", KR(ret));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::check_is_valid_name(query_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "RESULT_SCAN query_id");
    LOG_WARN("invalid result_scan query_id", KR(ret), K(query_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result_scan catalog SQL proxy is unavailable", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
                 *GCTX.sql_proxy_,
                 tenant_id,
                 false /* MySQL tenant */,
                 query_id,
                 alloc,
                 job_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = report_result_scan_table_not_exist(query_id);
    }
    LOG_WARN("failed to find async query scheduler record", KR(ret), K(tenant_id), K(query_id));
  } else if (OB_UNLIKELY(!job_info.is_async_query_job())) {
    ret = report_result_scan_table_not_exist(query_id);
    LOG_WARN("scheduler record is not an async query", KR(ret), K(job_info), K(query_id));
  } else if (OB_UNLIKELY(job_info.get_tenant_id() != tenant_id
                         || job_info.get_user_id() != current_user_id)) {
    // RESULT_SCAN is owner-only. Do not use the scheduler's administrative
    // job privilege helper here, and do not require SELECT on the private
    // physical result table.
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "RESULT_SCAN");
    LOG_WARN("result_scan owner check failed", KR(ret), K(tenant_id), K(current_user_id),
             "owner_user_id", job_info.get_user_id(), K(query_id));
  } else if (OB_FAIL(get_async_query_result_lifecycle(job_info, lifecycle))) {
    LOG_WARN("failed to derive async query result lifecycle",
             KR(ret), K(tenant_id), K(query_id), K(job_info));
  } else if (OB_UNLIKELY(!lifecycle.is_successful_)) {
    ret = report_result_scan_table_not_exist(query_id);
    if (lifecycle.is_finished_) {
      // Keep the original execution error private; SHOW JOB STATUS remains
      // the diagnostic surface.
      LOG_WARN("result_scan requested for async query without a successful result",
               KR(ret), K(tenant_id), K(query_id), K(job_info), K(lifecycle));
    } else {
      LOG_WARN("result_scan requested before async query completed",
               KR(ret), K(tenant_id), K(query_id), K(job_info), K(lifecycle));
    }
  } else if (OB_FAIL(async_query_config.deserialize(
                 job_info.get_job_config(), job_config_allocator))) {
    LOG_WARN("failed to parse async query result-store configuration", KR(ret), K(query_id));
  } else if (requested_chunk_no != OB_INVALID_INDEX_INT64
             && dbms_scheduler::ObAsyncQueryResultStore::EXTERNAL
                != async_query_config.get_result_store()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "RESULT_SCAN chunk_id for non-EXTERNAL async query results");
    LOG_WARN("chunked result_scan requested for a non-external result", KR(ret),
             K(query_id), K(requested_chunk_no),
             "result_store", async_query_config.get_result_store());
  } else if (dbms_scheduler::ObAsyncQueryResultStore::NATIVE
             == async_query_config.get_result_store()
             && OB_FAIL(check_async_query_result_not_expired(
                 tenant_id,
                 job_info.get_job_id(),
                 query_id,
                 lifecycle.completion_time_us_,
                 nullptr))) {
    LOG_WARN("native async query result is expired", KR(ret), K(query_id));
  } else if (dbms_scheduler::ObAsyncQueryResultStore::EXTERNAL
             == async_query_config.get_result_store()) {
    ObResultCatalogTableMetadata *catalog_metadata = nullptr;
    if (OB_NOT_NULL(cached_metadata)
        && OB_UNLIKELY(!share::is_result_catalog_table(cached_metadata->get_format_type()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cached catalog metadata is not a Result Catalog descriptor",
               KR(ret), K(tenant_id), K(query_id), KPC(cached_metadata));
    } else if (OB_NOT_NULL(cached_metadata)) {
      catalog_metadata = const_cast<ObResultCatalogTableMetadata *>(
          static_cast<const ObResultCatalogTableMetadata *>(cached_metadata));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(catalog_metadata)
        && OB_UNLIKELY(!catalog_metadata->matches(tenant_id, job_info.get_job_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cached Result Catalog metadata belongs to a different result",
               KR(ret), K(tenant_id), K(query_id), KPC(catalog_metadata));
    } else if (OB_SUCC(ret) && OB_ISNULL(catalog_metadata)) {
      if (OB_ISNULL(catalog_metadata = OB_NEWx(ObResultCatalogTableMetadata, &alloc, alloc))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate Result Catalog metadata", KR(ret), K(tenant_id), K(query_id));
      } else if (OB_FAIL(load_result_catalog_table_metadata(
                     tenant_id, job_info.get_job_id(), alloc, *catalog_metadata))) {
        const int load_ret = ret;
        if (OB_OBJECT_NOT_EXIST == load_ret) {
          ret = report_result_scan_table_not_exist(query_id);
        }
        LOG_WARN("failed to load Result Catalog metadata", KR(load_ret), KR(ret),
                 K(tenant_id), K(query_id));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(catalog_metadata)) {
      const ObResultTableMetadata &manifest = catalog_metadata->get_result_table_metadata();
      if (OB_FAIL(check_async_query_result_not_expired(
              tenant_id,
              job_info.get_job_id(),
              query_id,
              lifecycle.completion_time_us_,
              &manifest))) {
        LOG_WARN("external async query result is expired", KR(ret), K(query_id));
      } else {
        catalog_metadata->set_requested_chunk_no(requested_chunk_no);
      }
      if (OB_SUCC(ret) && requested_chunk_no != OB_INVALID_INDEX_INT64) {
        ObResultChunkLocation chunk_location;
        const int chunk_ret = manifest.get_chunk_location(requested_chunk_no, chunk_location);
        if (OB_ENTRY_NOT_EXIST == chunk_ret) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "RESULT_SCAN chunk_id");
        } else if (OB_NOT_SUPPORTED == chunk_ret) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                         "RESULT_SCAN chunk_id for this async query result");
        } else if (OB_SUCCESS != chunk_ret) {
          ret = chunk_ret;
          LOG_WARN("failed to resolve Result Catalog chunk", KR(chunk_ret), KR(ret),
                   K(query_id), K(requested_chunk_no));
        } else {
          LOG_TRACE("resolved Result Catalog chunk", K(query_id), K(requested_chunk_no),
                   K(chunk_location));
        }
      }
      LOG_TRACE("loaded Result Catalog manifest", K(tenant_id), K(query_id),
               "job_id", catalog_metadata->get_job_id(),
               "partition_count", manifest.get_partitions().count(),
               "file_count", manifest.get_file_count(),
               "row_count", manifest.get_row_count(),
               K(requested_chunk_no));
    }
    if (OB_SUCC(ret)) {
      const int schema_ret = build_parquet_result_table_schema(
          tenant_id,
          job_info.get_database_id(),
          job_info.get_job_id(),
          *catalog_metadata,
          alloc,
          allocated_result_table_schema);
      if (OB_SUCCESS != schema_ret) {
        if (OB_NOT_SUPPORTED == schema_ret) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "RESULT_SCAN result format on this server");
        } else {
          ret = schema_ret;
        }
        LOG_WARN("failed to resolve parquet result through Result Catalog",
                 KR(schema_ret), KR(ret), K(tenant_id), K(query_id));
      } else if (OB_ISNULL(allocated_result_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parquet result schema is null", KR(ret), K(tenant_id), K(query_id));
      } else if (OB_NOT_NULL(sql_schema_guard)) {
        const uint64_t mock_table_id = sql_schema_guard->get_next_mocked_schema_id();
        if (OB_UNLIKELY(!is_external_object_id(mock_table_id))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("no mocked schema id is available for parquet result",
                   KR(ret), K(mock_table_id), K(query_id));
        } else if (OB_FAIL(rebind_parquet_result_table_schema(
                       *allocated_result_table_schema,
                       tenant_id,
                       job_info.get_database_id(),
                       mock_table_id))) {
          LOG_WARN("failed to bind parquet result schema to statement",
                   KR(ret), K(mock_table_id), K(query_id));
        } else if (OB_FAIL(sql_schema_guard->add_mocked_table_schema(
                       *allocated_result_table_schema))) {
          LOG_WARN("failed to register parquet result schema",
                   KR(ret), K(mock_table_id), K(query_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      result_table_schema = allocated_result_table_schema;
      table_desc.table_id_ = result_table_schema->get_table_id();
      table_desc.catalog_table_metadata_ = catalog_metadata;
    }
  } else if (OB_FAIL(job_info.build_async_query_result_table_name(result_table_name))) {
    LOG_WARN("failed to build physical result table name", KR(ret), K(job_info));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is unavailable for native result", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get result_scan schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_database_id(
                 tenant_id,
                 dbms_scheduler::ObDBMSSchedJobInfo::get_async_query_result_database_name(),
                 result_database_id))) {
    LOG_WARN("failed to find async result database", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == result_database_id)) {
    ret = report_result_scan_table_not_exist(query_id);
    LOG_WARN("async result database does not exist", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id,
                 result_database_id,
                 result_table_name.string(),
                 false /* is_index */,
                 result_table_schema))) {
    LOG_WARN("failed to find physical async result table", KR(ret), K(tenant_id),
             K(result_database_id), K(result_table_name));
  } else if (OB_ISNULL(result_table_schema)) {
    ret = report_result_scan_table_not_exist(query_id);
    LOG_WARN("physical async result table schema is null", KR(ret), K(result_table_name));
  } else {
    table_desc.table_id_ = result_table_schema->get_table_id();
  }

  for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(result_table_schema)
                        && i < result_table_schema->get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = result_table_schema->get_column_schema_by_idx(i);
    ObResultScanColDesc column_desc;
    char *column_name = nullptr;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result column schema is null", KR(ret), K(i), K(result_table_name));
    } else if (column_schema->is_hidden()) {
      // Hidden storage columns are not part of the published SELECT schema.
    } else if (OB_FAIL(resolve_result_subschema(*column_schema, session,
                                               column_desc.subschema_id))) {
      LOG_WARN("failed to resolve result column subschema metadata", KR(ret), K(i));
    } else if (OB_ISNULL(column_name = static_cast<char *>(
                   alloc.alloc(column_schema->get_column_name_str().length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate result column name", KR(ret), K(i));
    } else {
      const ObString &name = column_schema->get_column_name_str();
      MEMCPY(column_name, name.ptr(), name.length());
      column_desc.col_name.assign_ptr(column_name, name.length());
      column_desc.column_id = column_schema->get_column_id();
      column_desc.ob_type = column_schema->get_data_type();
      column_desc.accuracy = column_schema->get_accuracy();
      column_desc.cs_type = column_schema->get_collation_type();
      if (OB_FAIL(col_descs.push_back(column_desc))) {
        LOG_WARN("failed to add result column descriptor", KR(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    table_desc.table_schema_ = result_table_schema;
  }
  return ret;
}

int ObResultScanCatalog::build_show_columns_sql(
    const ::_ParseNode *table_collection_expr,
    const ObSQLSessionInfo *session,
    ObIAllocator &allocator,
    const bool is_full,
    ObString &show_sql)
{
  int ret = OB_SUCCESS;
  ObString query_id;
  int64_t requested_chunk_no = OB_INVALID_INDEX_INT64;
  ObResultScanTableDesc result_table;
  ObSEArray<ObResultScanColDesc, 8> col_descs;
  const ObTableSchema *table_schema = nullptr;
  ObSqlString generated_sql;
  bool is_first = true;
  show_sql.reset();
  if (OB_FAIL(extract_result_scan_show_args(
          static_cast<const ParseNode *>(table_collection_expr), query_id, requested_chunk_no))) {
    LOG_WARN("failed to extract RESULT_SCAN arguments for DESC", KR(ret));
  } else if (OB_FAIL(get_result_table(query_id,
                                      session,
                                      nullptr /* sql_schema_guard */,
                                      allocator,
                                      result_table,
                                      col_descs,
                                      nullptr /* cached_metadata */,
                                      requested_chunk_no))) {
    LOG_WARN("failed to resolve Result Catalog schema for DESC", KR(ret), K(query_id));
  } else if (OB_ISNULL(table_schema = result_table.table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Result Catalog schema is null for DESC", KR(ret), K(query_id));
  } else if (OB_FAIL(generated_sql.append("SELECT * FROM ("))) {
    LOG_WARN("failed to start Result Catalog DESC SQL", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); ++i) {
      const ObColumnSchemaV2 *column = table_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Result Catalog DESC schema has null column", KR(ret), K(i), K(query_id));
      } else if (column->is_hidden()) {
        // Storage-only file/line columns are not part of the published result
        // schema, including for SHOW EXTENDED.
      } else if (OB_FAIL(append_result_show_column_row(generated_sql,
                                                       *column,
                                                       session,
                                                       allocator,
                                                       is_full,
                                                       is_first))) {
        LOG_WARN("failed to append Result Catalog DESC column", KR(ret), K(i), K(query_id));
      } else {
        is_first = false;
      }
    }
    if (OB_SUCC(ret) && is_first) {
      // SELECT has no zero-column result, but retain a valid empty SHOW shape
      // if a malformed legacy result nevertheless reaches this point.
      if (OB_FAIL(generated_sql.append(
              is_full
                  ? "SELECT '' AS `Field`, '' AS `Type`, '' AS `Collation`, '' AS `Null`, "
                    "'' AS `Key`, NULL AS `Default`, '' AS `Extra`, '' AS `Privileges`, "
                    "'' AS `Comment` WHERE 1 = 0"
                  : "SELECT '' AS `Field`, '' AS `Type`, '' AS `Null`, '' AS `Key`, "
                    "NULL AS `Default`, '' AS `Extra` WHERE 1 = 0"))) {
        LOG_WARN("failed to build empty Result Catalog DESC SQL", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(generated_sql.append(") subquery_alias"))) {
      LOG_WARN("failed to finish Result Catalog DESC SQL", KR(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(ob_write_string(
            allocator, generated_sql.string(), show_sql, true /* c_style */))) {
      LOG_WARN("failed to persist Result Catalog DESC SQL", KR(ret), K(query_id));
    }
  }
  return ret;
}

int ObResultScanCatalog::build_show_create_table_sql(
    const ::_ParseNode *table_collection_expr,
    const ObSQLSessionInfo *session,
    ObIAllocator &allocator,
    ObString &show_sql)
{
  int ret = OB_SUCCESS;
  ObString query_id;
  int64_t requested_chunk_no = OB_INVALID_INDEX_INT64;
  ObResultScanTableDesc result_table;
  ObSEArray<ObResultScanColDesc, 8> col_descs;
  ObSqlString generated_sql;
  ObString logical_table_name;
  ObString create_sql;
  show_sql.reset();
  if (OB_FAIL(extract_result_scan_show_args(
          static_cast<const ParseNode *>(table_collection_expr), query_id, requested_chunk_no))) {
    LOG_WARN("failed to extract RESULT_SCAN arguments for SHOW CREATE", KR(ret));
  } else if (OB_FAIL(get_result_table(query_id,
                                      session,
                                      nullptr /* sql_schema_guard */,
                                      allocator,
                                      result_table,
                                      col_descs,
                                      nullptr /* cached_metadata */,
                                      requested_chunk_no))) {
    LOG_WARN("failed to resolve Result Catalog schema for SHOW CREATE", KR(ret), K(query_id));
  } else if (OB_NOT_NULL(result_table.catalog_table_metadata_)) {
    if (OB_FAIL(build_external_result_create_sql(query_id,
                                                  result_table,
                                                  session,
                                                  allocator,
                                                  logical_table_name,
                                                  create_sql))) {
      LOG_WARN("failed to build external Result Catalog SHOW CREATE SQL", KR(ret), K(query_id));
    } else if (OB_FAIL(generated_sql.append("SELECT "))) {
    } else if (OB_FAIL(append_show_sql_string_literal(generated_sql, logical_table_name))) {
    } else if (OB_FAIL(generated_sql.append(" AS `Table`, "))) {
    } else if (OB_FAIL(append_show_sql_string_literal(generated_sql, create_sql))) {
    } else if (OB_FAIL(generated_sql.append(" AS `Create Table`"))) {
    }
  } else if (OB_ISNULL(result_table.table_schema_)
             || OB_UNLIKELY(OB_INVALID_ID == result_table.table_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid native Result Catalog table for SHOW CREATE", KR(ret),
             KP(result_table.table_schema_), K(result_table.table_id_), K(query_id));
  } else if (OB_FAIL(generated_sql.append_fmt(
                 "SELECT table_name AS `Table`, create_table AS `Create Table` "
                 "FROM `%s`.`%s` WHERE table_id = %lu",
                 OB_SYS_DATABASE_NAME,
                 OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TNAME,
                 static_cast<unsigned long>(result_table.table_id_)))) {
    LOG_WARN("failed to build native Result Catalog SHOW CREATE SQL", KR(ret), K(query_id));
  }
  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(
          allocator, generated_sql.string(), show_sql, true /* c_style */))) {
    LOG_WARN("failed to persist Result Catalog SHOW CREATE SQL", KR(ret), K(query_id));
  }
  return ret;
}

int ObResultScanCatalog::prepare_lake_scan(
    const share::ObILakeTableMetadata *catalog_metadata,
    ObIAllocator &allocator,
    const uint64_t table_id,
    const uint64_t ref_table_id,
    ObIArray<ObHiveFileDesc> &file_descs,
    ObILakeTableFilePruner *&file_pruner)
{
  int ret = OB_SUCCESS;
  const ObResultCatalogTableMetadata *result_metadata = nullptr;
  const ObResultTableMetadata *manifest = nullptr;
  int64_t requested_chunk_no = OB_INVALID_INDEX_INT64;
  ObResultChunkLocation chunk_location;
  file_pruner = nullptr;
  if (OB_ISNULL(catalog_metadata)
      || !share::is_result_catalog_table(catalog_metadata->get_format_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog metadata for Result Catalog lake scan",
             KR(ret), K(table_id), K(ref_table_id), KP(catalog_metadata));
  } else if (FALSE_IT(result_metadata =
                          static_cast<const ObResultCatalogTableMetadata *>(catalog_metadata))) {
  } else if (FALSE_IT(manifest = &result_metadata->get_result_table_metadata())) {
  } else if (OB_FAIL(manifest->validate())) {
    LOG_WARN("invalid Result Catalog manifest", KR(ret), K(table_id), K(ref_table_id),
             "job_id", result_metadata->get_job_id());
  } else if (FALSE_IT(requested_chunk_no = result_metadata->get_requested_chunk_no())) {
  } else if (requested_chunk_no != OB_INVALID_INDEX_INT64
             && OB_FAIL(manifest->get_chunk_location(requested_chunk_no, chunk_location))) {
    LOG_WARN("failed to resolve Result Catalog chunk location", KR(ret), K(table_id),
             K(ref_table_id), K(requested_chunk_no));
  } else if (OB_SUCC(ret) && requested_chunk_no != OB_INVALID_INDEX_INT64
             && OB_UNLIKELY(chunk_location.partition_idx_ < 0
                             || chunk_location.file_idx_ < 0
                             || chunk_location.row_group_idx_ < 0
                             || chunk_location.row_group_idx_ == INT64_MAX
                             || chunk_location.row_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid Result Catalog chunk location", KR(ret), K(table_id), K(ref_table_id),
             K(requested_chunk_no), K(chunk_location));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < manifest->get_partitions().count(); ++i) {
    if (requested_chunk_no != OB_INVALID_INDEX_INT64 && i != chunk_location.partition_idx_) {
      continue;
    }
    const ObResultPartitionMetadata &partition = manifest->get_partitions().at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < partition.files_.count(); ++j) {
      if (requested_chunk_no != OB_INVALID_INDEX_INT64 && j != chunk_location.file_idx_) {
        continue;
      }
      const ObResultFileMetadata &result_file = partition.files_.at(j);
      ObHiveFileDesc file_desc;
      if (OB_UNLIKELY(result_file.path_.empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("empty file path in Result Catalog manifest", KR(ret), K(i), K(j));
      } else {
        file_desc.part_id_ = partition.partition_id_;
        file_desc.file_size_ = result_file.byte_count_;
        // The immutable manifest does not record object mtime. The existing
        // Parquet reader obtains it for this exact file when needed.
        file_desc.modify_ts_ = 0;
        if (requested_chunk_no != OB_INVALID_INDEX_INT64) {
          // The private Result reader's task range is one-based, while the
          // Result Catalog manifest stores zero-based Parquet row-group IDs.
          file_desc.first_lineno_ = chunk_location.row_group_idx_ + 1;
          file_desc.last_lineno_ = chunk_location.row_group_idx_ + 1;
          // The manifest stores the physical Parquet file size, not a
          // row-group byte map.  Estimate the selected row group's bytes by
          // its exact row-count share of that file, so the external-table
          // cost model sees the same approximate input size as this pruned
          // scan instead of the entire file.
          if (OB_UNLIKELY(result_file.row_count_ <= 0
                          || chunk_location.row_count_ > result_file.row_count_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid Result Catalog chunk size estimate input", KR(ret),
                     K(requested_chunk_no), K(chunk_location), K(result_file));
          } else if (result_file.byte_count_ > 0) {
            const double estimated_bytes =
                static_cast<double>(result_file.byte_count_)
                * static_cast<double>(chunk_location.row_count_)
                / static_cast<double>(result_file.row_count_);
            file_desc.file_size_ = std::max<int64_t>(
                1, static_cast<int64_t>(estimated_bytes + 0.5));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, result_file.path_, file_desc.file_path_))) {
          LOG_WARN("failed to copy Result Catalog file path", KR(ret), K(i), K(j));
        } else if (OB_SUCC(ret) && OB_FAIL(file_descs.push_back(file_desc))) {
          LOG_WARN("failed to append Result Catalog file", KR(ret), K(i), K(j));
        }
      }
    }
  }

  ObResultFilePruner *result_file_pruner = nullptr;
  if (OB_SUCC(ret)
      && OB_ISNULL(result_file_pruner = OB_NEWx(ObResultFilePruner, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Result Catalog file pruner", KR(ret));
  } else if (OB_SUCC(ret)
             && OB_FAIL(result_file_pruner->init(table_id, ref_table_id))) {
    LOG_WARN("failed to initialize Result Catalog file pruner", KR(ret), K(table_id),
             K(ref_table_id));
  } else if (OB_SUCC(ret)) {
    file_pruner = result_file_pruner;
    LOG_TRACE("prepared Result Catalog lake scan", K(table_id), K(ref_table_id),
             "job_id", result_metadata->get_job_id(),
             "manifest_file_count", manifest->get_file_count(),
             "selected_file_count", file_descs.count(),
             K(requested_chunk_no),
             "selected_row_group", requested_chunk_no == OB_INVALID_INDEX_INT64
                                     ? OB_INVALID_INDEX_INT64 : chunk_location.row_group_idx_,
             "selected_chunk_row_count", requested_chunk_no == OB_INVALID_INDEX_INT64
                                             ? 0 : chunk_location.row_count_);
  }
  return ret;
}

int ObResultScanCatalog::get_result_row_count(
    const share::ObILakeTableMetadata *catalog_metadata,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  row_count = 0;
  if (OB_ISNULL(catalog_metadata)
      || !share::is_result_catalog_table(catalog_metadata->get_format_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog metadata for Result Catalog cardinality", KR(ret),
             KP(catalog_metadata));
  } else {
    const ObResultCatalogTableMetadata *result_metadata =
        static_cast<const ObResultCatalogTableMetadata *>(catalog_metadata);
    const int64_t requested_chunk_no = result_metadata->get_requested_chunk_no();
    if (requested_chunk_no == OB_INVALID_INDEX_INT64) {
      row_count = result_metadata->get_result_table_metadata().get_row_count();
    } else {
      ObResultChunkLocation chunk_location;
      if (OB_FAIL(result_metadata->get_result_table_metadata().get_chunk_location(
              requested_chunk_no, chunk_location))) {
        LOG_WARN("failed to get Result Catalog chunk cardinality", KR(ret),
                 K(requested_chunk_no), "job_id", result_metadata->get_job_id());
      } else {
        row_count = chunk_location.row_count_;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "share/search_index/ob_search_index_builder_util.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "share/search_index/ob_search_index_config_filter.h"
#include "share/search_index/ob_search_index_config.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"
#include "share/ob_index_builder_util.h"
#include "common/ob_version_def.h"
#include "share/ob_server_struct.h"
#include "rootserver/ob_root_service.h"
#include "lib/udt/ob_array_utils.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;

namespace share
{

int ObSearchIndexBuilderUtil::resolve_search_index_args(
    const ObCreateIndexArg &arg, ObSearchIndexRequirements &requirements)
{
  int ret = OB_SUCCESS;
  requirements = ObSearchIndexRequirements();
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
    ObSearchIndexConfig config;
    const ObString &comment = arg.index_columns_.at(i).column_comment_;
    if (OB_FAIL(config.decode(comment))) {
      LOG_WARN("failed to decode search column configuration", K(ret), K(i));
    } else {
      const ObIArray<ObSearchIndexModeConfig> &modes = config.get_modes();
      for (int64_t j = 0; OB_SUCC(ret) && j < modes.count(); ++j) {
        const ObSearchIndexModeConfig &mode = modes.at(j);
        ObSearchIndexConfigFilter filter(OB_SERVER_TENANT_ID);
        if (OB_FAIL(filter.init_from_comment(config.get_comment(), j))) {
          LOG_WARN("failed to decode search mode filter", K(ret), K(i), K(j));
        } else {
          requirements.has_nested_ |= filter.has_nested_paths();
          requirements.has_data_ |= mode.mode_ == ObSearchIndexModeConfig::VALUE;
          requirements.has_fulltext_ |= mode.is_fulltext();
          requirements.has_text_match_ |= mode.mode_ == ObSearchIndexModeConfig::FULLTEXT_MATCH;
          requirements.has_text_match_phrase_ |= mode.mode_ == ObSearchIndexModeConfig::FULLTEXT_PHRASE;
        }
      }
    }
  }
  if (OB_SUCC(ret) && requirements.has_fulltext_) {
    if (!is_async_search_index(arg.index_type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "synchronous search index FULLTEXT is");
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::resolve_search_index_args(const ObCreateIndexArg &arg)
{
  ObSearchIndexRequirements requirements;
  return resolve_search_index_args(arg, requirements);
}

int ObSearchIndexBuilderUtil::check_fulltext_data_version(const ObCreateIndexArg &arg,
                                                       const uint64_t tenant_data_version)
{
  int ret = OB_SUCCESS;
  if (is_async_search_index(arg.index_type_) && tenant_data_version < DATA_VERSION_4_6_3_1) {
    ObSearchIndexRequirements requirements;
    if (OB_FAIL(resolve_search_index_args(arg, requirements))) {
      LOG_WARN("failed to resolve search index configuration", K(ret));
    } else if (requirements.has_fulltext_ || is_search_text_index(arg.index_type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("JSON FULLTEXT search index not supported before 4.6.3.1",
               K(ret), K(tenant_data_version), K(arg.index_type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "JSON FULLTEXT search index before version 4.6.3.1");
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::generate_search_index_name(
    obrpc::ObCreateIndexArg &arg,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  char *name_buf = nullptr;
  share::schema::ObIndexType type = arg.index_type_;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret));
  } else if (!is_search_aux_index(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(name_buf = static_cast<char *>(allocator->alloc(OB_MAX_TABLE_NAME_LENGTH)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(ret));
  } else {
    MEMSET(name_buf, 0, OB_MAX_TABLE_NAME_LENGTH);
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(name_buf, OB_MAX_TABLE_NAME_LENGTH, pos,
                                "%.*s_%s",
                                arg.index_name_.length(),
                                arg.index_name_.ptr(),
                                !is_search_text_index(type) ? "search_data"
                                : is_async_search_text_match_index(type)
                                    ? "search_text_match" : "search_text_match_phrase"))) {
      LOG_WARN("failed to format search data index name", K(ret));
    } else {
      arg.index_name_.assign_ptr(name_buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::append_search_index_arg(
    const ObCreateIndexArg &index_arg,
    const ObSearchIndexRequirements &requirements,
    ObIAllocator *allocator,
    ObIArray<ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator) || !is_search_def_index(index_arg.index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for search index expansion", K(ret), KP(allocator), K(index_arg.index_type_));
  } else {
    const ObIndexType types[] = {
      is_async_search_def_index(index_arg.index_type_)
          ? INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL : INDEX_TYPE_SEARCH_DATA_INDEX_LOCAL,
      INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL
    };
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(types); ++i) {
      if ((i == 0 && !requirements.has_data_)
          || (i == 1 && !requirements.has_text_match_) || (i == 2 && !requirements.has_text_match_phrase_)) {
        continue;
      }
      ObCreateIndexArg child;
      if (OB_FAIL(child.assign(index_arg))) {
        LOG_WARN("failed to copy search definition", K(ret));
      } else {
        child.index_type_ = types[i];
        if (OB_FAIL(generate_search_index_name(child, allocator))) {
          LOG_WARN("failed to generate search auxiliary name", K(ret), K(types[i]));
        } else if (OB_FAIL(index_arg_list.push_back(child))) {
          LOG_WARN("failed to append search auxiliary argument", K(ret), K(types[i]));
        }
      }
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::set_search_index_table_columns(
    const obrpc::ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() || !share::schema::is_search_aux_index(arg.index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for search index", K(ret), K(data_schema), K(arg.index_type_));
  } else if (is_search_text_index(arg.index_type_)) {
    if (OB_FAIL(set_search_text_table_columns(arg, data_schema, index_schema))) {
      LOG_WARN("failed to set search text table columns", K(ret));
    }
  } else {
    int64_t rowkey_pos = 0;
    const int64_t tenant_id = data_schema.get_tenant_id();
    const int64_t ts_suffix = ObTimeUtility::current_time();

    // Pre-detect NESTED_PATHS to decide whether to add the nested_key rowkey column.
    // VALUE length already reserves nested_key space unconditionally (see SEARCH_INDEX_VALUE_LENGTH),
    // so no per-index value-length adjustment is needed here.
    ObSearchIndexRequirements requirements;
    if (OB_FAIL(resolve_search_index_args(arg, requirements))) {
      LOG_WARN("invalid search index configuration", K(ret));
    }
    const bool has_nested_paths = requirements.has_nested_;
    const int64_t value_len = SEARCH_INDEX_VALUE_LENGTH;

    struct ColumnDef {
      const char *name_prefix;
      ObObjType type;
      int64_t length;
      bool is_binary;
      bool is_rowkey;
    } columns[] = {
      {"COL_IDX",    ObIntType,     0,                        false, true},
      {"PATH",       ObVarcharType, SEARCH_INDEX_PATH_LENGTH, true,  true},
      {"VALUE",      ObVarcharType, value_len,                true,  true},
      {"DOC_ID",     ObUInt64Type,  0,                        false, true},
    };
    const int64_t column_cnt = static_cast<int64_t>(sizeof(columns) / sizeof(columns[0]));
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      const ColumnDef &c = columns[i];
      char generated_name[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
      int64_t name_pos = 0;
      if (OB_FAIL(databuff_printf(generated_name,
                                  OB_MAX_COLUMN_NAME_LENGTH,
                                  name_pos,
                                  "__%s_%ld",
                                  c.name_prefix,
                                  ts_suffix))) {
        LOG_WARN("failed to generate search column name", K(ret), K(i), K(c.name_prefix), K(ts_suffix));
      } else if (name_pos <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected generated search column name", K(ret), K(i), K(c.name_prefix), K(ts_suffix));
      } else if (OB_FAIL(add_search_index_column(data_schema,
                                                 index_schema,
                                                 tenant_id,
                                                 generated_name,
                                                 c.type,
                                                 c.length,
                                                 c.is_binary,
                                                 c.is_rowkey,
                                                 rowkey_pos))) {
        LOG_WARN("failed to add search column", K(ret), K(i), K(generated_name));
      }
    }

    // Synchronous search keeps the scalar nested key as the fifth rowkey. Async search folds all
    // scalar keys of one (term, doc_id) into an opaque list stored as a posting value, so its schema
    // keeps only the first four columns as rowkeys.
    if (OB_SUCC(ret) && has_nested_paths) {
      char generated_name[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
      int64_t name_pos = 0;
      const bool is_async_data_index = is_async_search_data_index(arg.index_type_);
      if (OB_FAIL(databuff_printf(generated_name, OB_MAX_COLUMN_NAME_LENGTH, name_pos,
                                  "__NESTED_KEY_%ld", ts_suffix))) {
        LOG_WARN("failed to generate nested key column name", K(ret), K(ts_suffix));
      } else if (OB_FAIL(add_search_index_column(data_schema, index_schema, tenant_id,
                                                 generated_name,
                                                 ObVarcharType,
                                                 SEARCH_INDEX_NESTED_KEY_LENGTH,
                                                 true /*is_binary*/,
                                                 !is_async_data_index /*is_rowkey*/,
                                                 rowkey_pos))) {
        LOG_WARN("failed to add nested key column", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      index_schema.set_rowkey_column_num(rowkey_pos);
      index_schema.set_index_column_num(rowkey_pos);
      if (OB_FAIL(index_schema.sort_column_array_by_column_id())) {
        LOG_WARN("failed to sort columns", K(ret));
      }
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::set_search_text_table_columns(
    const ObCreateIndexArg &arg, const ObTableSchema &data_schema, ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  ObSearchIndexRequirements requirements;
  const bool phrase = is_async_search_text_match_phrase_index(arg.index_type_);
  if (OB_FAIL(resolve_search_index_args(arg, requirements))) {
    LOG_WARN("invalid search text configuration", K(ret));
  } else if (!(phrase ? requirements.has_text_match_phrase_ : requirements.has_text_match_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("search text table is not required by the definition", K(ret), K(arg.index_type_));
  } else {
    // Column IDs and roles belong to the physical Text schema, not the source JSON columns.
    // Keep key capacity within the existing Search Data rowkey limit.
    // TOKEN stores the encoded term key with binary comparison, like Search Data VALUE.
    const struct {
      const char *name_;
      ObObjType type_;
      int64_t length_;
    } columns[] = {
      {"COL_IDX", ObUInt64Type, 0},
      {"PATH", ObVarcharType, SEARCH_INDEX_PATH_LENGTH},
      {"TOKEN", ObVarcharType, SEARCH_INDEX_VALUE_LENGTH},
      {"DOC_ID", ObUInt64Type, 0},
      {"TOKEN_FREQ", ObUInt64Type, 0},
      {"DOC_LENGTH", ObUInt64Type, 0},
      {"POSITION_LIST", ObVarcharType, 16 * 1024}
    };
    static_assert(ARRAYSIZEOF(columns) == blocksstable::SEARCH_TEXT_COL_CNT_WITH_POS,
        "Search Text schema must match the shared column layout");
    const int64_t column_count = phrase ? blocksstable::SEARCH_TEXT_COL_CNT_WITH_POS
                                       : blocksstable::SEARCH_TEXT_COL_CNT_WITHOUT_POS;
    int64_t rowkey_pos = 0;
    const int64_t suffix = ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      char name[OB_MAX_COLUMN_NAME_LENGTH] = {};
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(name, sizeof(name), pos, "__%s_%ld", columns[i].name_, suffix))) {
        LOG_WARN("failed to format search text column name", K(ret));
      } else if (OB_FAIL(add_search_index_column(data_schema, index_schema, data_schema.get_tenant_id(),
          name, columns[i].type_, columns[i].length_, true,
          i < blocksstable::SEARCH_TEXT_ROWKEY_CNT, rowkey_pos))) {
        LOG_WARN("failed to add search text column", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      index_schema.set_rowkey_column_num(rowkey_pos);
      index_schema.set_index_column_num(rowkey_pos);
      if (OB_FAIL(index_schema.sort_column_array_by_column_id())) {
        LOG_WARN("failed to sort search text columns", K(ret));
      }
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::check_single_layer_array_for_search_index(
    const share::schema::ObColumnSchemaV2 &column_schema,
    ObIAllocator *allocator,
    bool &is_supported)
{
  int ret = OB_SUCCESS;
  is_supported = false;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (!column_schema.is_collection()) {
    // not collection column, ignore
    is_supported = false;
  } else {
    const ObIArray<ObString> &type_infos = column_schema.get_extended_type_info();
    ObSqlCollectionInfo *coll_info = nullptr;
    if (OB_FAIL(ObArrayUtil::get_collection_info(type_infos, *allocator, coll_info))) {
      LOG_WARN("failed to get collection info", K(ret), K(type_infos));
    } else if (OB_ISNULL(coll_info) || OB_ISNULL(coll_info->collection_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("collection info or meta is null", K(ret), KP(coll_info));
    } else if (coll_info->collection_meta_->type_id_ != ObNestedType::OB_ARRAY_TYPE) {
      is_supported = false;
    } else {
      uint32_t depth = 0;
      (void) coll_info->collection_meta_->get_basic_meta(depth);
      is_supported = (depth == 1);
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::add_search_index_column(
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema,
    const int64_t tenant_id,
    const char *name,
    ObObjType type,
    const int64_t length,
    const bool is_binary,
    const bool is_rowkey,
    int64_t &rowkey_pos)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 col;
  col.set_tenant_id(tenant_id);
  col.set_column_name(name);
  col.set_data_type(type);
  col.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[type]);
  if (type == ObVarcharType) {
    col.set_data_length(length);
    if (is_binary) {
      col.set_collation_type(CS_TYPE_BINARY);
      col.set_charset_type(ObCharset::charset_type_by_coll(data_schema.get_collation_type()));
    } else {
      col.set_collation_type(data_schema.get_collation_type());
    }
  } else {
    col.set_collation_type(CS_TYPE_BINARY);
  }
  col.set_rowkey_position(is_rowkey ? (++rowkey_pos) : 0);
  col.set_index_position(is_rowkey ? rowkey_pos : 0);
  col.set_order_in_rowkey(common::ObOrderType::ASC);
  col.set_prev_column_id(UINT64_MAX);
  col.set_next_column_id(UINT64_MAX);
  col.set_tbl_part_key_pos(0);
  col.set_table_id(OB_INVALID_ID);
  col.set_is_hidden(false);

  uint64_t new_col_id = index_schema.get_max_used_column_id() + 1;
  col.set_column_id(new_col_id);
  if (OB_FAIL(index_schema.add_column(col))) {
    LOG_WARN("add search column failed", K(ret), K(col));
  }
  return ret;
}

int ObSearchIndexBuilderUtil::get_dropping_search_data_index_invisiable_index_schema(
    const uint64_t tenant_id,
    const ObTableSchema &index_table_schema,
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObIArray<share::schema::ObTableSchema> &new_aux_schemas)
{
  int ret = OB_SUCCESS;
  if (index_table_schema.is_search_def_index()) {
    const ObIArray<share::schema::ObAuxTableMetaInfo> &indexs = index_table_schema.get_simple_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < indexs.count(); ++i) {
      const ObTableSchema *data_index_schema = NULL;
      const share::schema::ObAuxTableMetaInfo &info = indexs.at(i);
      if ((!index_table_schema.is_async_search_def_index()
           && INDEX_TYPE_SEARCH_DATA_INDEX_LOCAL != info.index_type_)
          || (index_table_schema.is_async_search_def_index()
              && !share::schema::is_async_search_aux_index(info.index_type_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect search child index", KR(ret), K(info.index_type_));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                       info.table_id_,
                                                       data_index_schema))) {
        LOG_WARN("fail to get search data index schema", K(ret),
                  "data_index_table_id", info.table_id_);
      } else if (OB_ISNULL(data_index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, data search index schema is nullptr", K(ret), K(info));
      } else if (OB_FAIL(new_aux_schemas.push_back(*data_index_schema))) {
        LOG_WARN("fail to push doc rowkey table schema", K(ret), KPC(data_index_schema));
      }
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::get_search_index_column_name(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    ObIArray<ObString> &col_names,
    ObIArray<ObString> &col_comments)
{
  int ret = OB_SUCCESS;
  col_names.reset();
  col_comments.reset();
  if (!index_table_schema.is_search_index()
      && !index_table_schema.is_async_search_def_index()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, not a search def index", K(ret), K(index_table_schema));
  } else {
    // Search index uses the original create index column names
    // stored in the index_schema's index_columns
    for (int64_t i = 0; OB_SUCC(ret) && i < index_table_schema.get_column_count(); i++) {
      const ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = index_table_schema.get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(i), K(index_table_schema));
      } else if (col_schema->get_index_position() > 0) {
        // This is an index column, get the original column name from data table
        const ObColumnSchemaV2 *data_col_schema = data_table_schema.get_column_schema(col_schema->get_column_id());
        if (OB_ISNULL(data_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected data column not found", K(ret), K(col_schema->get_column_id()), K(data_table_schema));
        } else if (OB_FAIL(col_names.push_back(data_col_schema->get_column_name_str()))) {
          LOG_WARN("fail to push back col names", K(ret), K(data_col_schema->get_column_name_str()));
        // The per-column search config is repurposed into the index column's comment. It must be
        // carried out together with the column name, otherwise callers rebuilding the index arg
        // would silently lose the config (e.g. NESTED_PATHS, which decides whether the
        // search_data table needs the extra NESTED_KEY rowkey column).
        } else if (OB_FAIL(col_comments.push_back(col_schema->get_comment_str()))) {
          LOG_WARN("fail to push back col comments", K(ret), K(col_schema->get_comment_str()));
        }
      }
    }
  }
  return ret;
}

const ObColumnSchemaV2 *ObSearchIndexBuilderUtil::get_nested_key_column_schema(
    const ObTableSchema &index_table_schema)
{
  const ObColumnSchemaV2 *nested_key_column = nullptr;
  if (index_table_schema.is_search_data_index()
      && SEARCH_INDEX_KEY_CNT_WITH_NK == index_table_schema.get_column_count()) {
    // The nested key is the fifth generated search-index column. Synchronous indexes use it as a
    // rowkey, while asynchronous indexes use it as a posting value.
    nested_key_column = index_table_schema.get_column_schema_by_idx(SEARCH_INDEX_NESTED_KEY);
  }
  return nested_key_column;
}

} // namespace share
}//end namespace oceanbase

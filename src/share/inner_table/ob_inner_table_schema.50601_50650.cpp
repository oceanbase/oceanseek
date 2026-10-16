/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_inner_table_schema.h"

#include "share/schema/ob_schema_macro_define.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_table_schema.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
namespace share
{

int ObInnerTableSchema::all_shared_gc_task_history_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SHARED_GC_TASK_HISTORY_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SHARED_GC_TASK_HISTORY_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SHARED_GC_TASK_HISTORY_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_SHARED_GC_TASK_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_shared_gc_control_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SHARED_GC_CONTROL_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SHARED_GC_CONTROL_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SHARED_GC_CONTROL_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_SHARED_GC_CONTROL_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_shared_meta_reserved_scn_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SHARED_META_RESERVED_SCN_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SHARED_META_RESERVED_SCN_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SHARED_META_RESERVED_SCN_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_SHARED_META_RESERVED_SCN_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_agent_db_background_task_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_AGENT_DB_BACKGROUND_TASK_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_AGENT_DB_BACKGROUND_TASK_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_AGENT_DB_BACKGROUND_TASK_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_AGENT_DB_BACKGROUND_TASK_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_agent_db_background_task_history_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_fork_table_tablet_task_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_FORK_TABLE_TABLET_TASK_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FORK_TABLE_TABLET_TASK_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_FORK_TABLE_TABLET_TASK_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_FORK_TABLE_TABLET_TASK_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_fork_table_tablet_task_history_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_FORK_TABLE_TABLET_TASK_HISTORY_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FORK_TABLE_TABLET_TASK_HISTORY_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_FORK_TABLE_TABLET_TASK_HISTORY_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_FORK_TABLE_TABLET_TASK_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_fork_table_reserved_scn_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_FORK_TABLE_RESERVED_SCN_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FORK_TABLE_RESERVED_SCN_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_FORK_TABLE_RESERVED_SCN_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_FORK_TABLE_RESERVED_SCN_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_ai_batch_task_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_AI_BATCH_TASK_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_AI_BATCH_TASK_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_AI_BATCH_TASK_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_AI_BATCH_TASK_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_ai_batch_task_history_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_AI_BATCH_TASK_HISTORY_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_AI_BATCH_TASK_HISTORY_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_AI_BATCH_TASK_HISTORY_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_AI_BATCH_TASK_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_ai_model_provider_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_AI_MODEL_PROVIDER_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_AI_MODEL_PROVIDER_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_AI_MODEL_PROVIDER_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_AI_MODEL_PROVIDER_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_ai_model_provider_history_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_AI_MODEL_PROVIDER_HISTORY_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_AI_MODEL_PROVIDER_HISTORY_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_AI_MODEL_PROVIDER_HISTORY_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_AI_MODEL_PROVIDER_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_ai_model_profile_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_AI_MODEL_PROFILE_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_AI_MODEL_PROFILE_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_AI_MODEL_PROFILE_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_AI_MODEL_PROFILE_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_ai_gateway_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_AI_GATEWAY_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_AI_GATEWAY_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_AI_GATEWAY_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_AI_GATEWAY_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_ai_gateway_history_aux_lob_meta_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_AI_GATEWAY_HISTORY_AUX_LOB_META_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_AI_GATEWAY_HISTORY_AUX_LOB_META_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_col_idx", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      24, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("seq_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      8192, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("byte_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("char_len", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      262144, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lob_block_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_AI_GATEWAY_HISTORY_AUX_LOB_META_TID);
  table_schema.set_micro_index_clustered(false);
  table_schema.set_data_table_id(OB_ALL_AI_GATEWAY_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}


} // end namespace share
} // end namespace oceanbase

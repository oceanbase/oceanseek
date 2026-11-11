/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#define private public
#include "share/schema/ob_table_dml_param.h"
#include "share/schema/ob_column_schema.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace schema;

TEST(ObTableSchemaParam, test_serialize)
{
  ObArenaAllocator allocator1;
  ObArenaAllocator allocator2;
  ObTableSchemaParam old_param1(allocator1);
  ObTableSchemaParam new_param1(allocator1);
  ObTableSchemaParam new_param2(allocator2);

  old_param1.table_id_ = 1;
  old_param1.index_status_ = INDEX_STATUS_AVAILABLE;
  old_param1.def_type_ = TABLE_DEF_TYPE_INTERNAL;
  ObArenaAllocator allocator;
  int64_t buf_size = 1024;
  char *buf = (char*)allocator.alloc(buf_size);
  int64_t pos = 0;

  LOG_INFO("dump param", K(old_param1));
  ASSERT_EQ(OB_SUCCESS, old_param1.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, old_param1.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_param1.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param1), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, new_param2.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param2), K(pos), K(buf_size));
  ASSERT_EQ(old_param1.table_id_, new_param1.table_id_);
  ASSERT_EQ(old_param1.table_id_, new_param2.table_id_);
  ASSERT_EQ(TABLE_DEF_TYPE_INTERNAL, new_param1.get_def_type());
  ASSERT_EQ(TABLE_DEF_TYPE_INTERNAL, new_param2.get_def_type());

  new_param1.table_id_ = 2;
  new_param1.def_type_ = TABLE_DEF_TYPE_USER;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_param1.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, new_param2.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_param1.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(new_param1.table_id_, old_param1.table_id_);
  ASSERT_EQ(TABLE_DEF_TYPE_USER, old_param1.get_def_type());
  ASSERT_EQ(OB_SUCCESS, old_param1.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(new_param2.table_id_, old_param1.table_id_);
  ASSERT_EQ(TABLE_DEF_TYPE_INTERNAL, old_param1.get_def_type());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_param2.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param2), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, new_param2.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param2), K(pos), K(buf_size));

}

TEST(ObTableSchemaParam, definition_type_legacy_compatibility)
{
  ObArenaAllocator allocator;
  ObTableSchemaParam param(allocator);
  param.table_id_ = 500002;
  param.def_type_ = TABLE_DEF_TYPE_INTERNAL;

  char buf[4096];
  int64_t write_pos = 0;
  ASSERT_EQ(OB_SUCCESS, param.serialize(buf, sizeof(buf), write_pos));
  ASSERT_LE(write_pos, param.get_serialize_size());

  int64_t header_pos = 0;
  int64_t version = 0;
  int64_t payload_len = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, write_pos, header_pos, version));
  const int64_t length_pos = header_pos;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, write_pos, header_pos, payload_len));
  const int64_t def_type_len = serialization::encoded_length(param.get_def_type());
  const int64_t legacy_len = write_pos - def_type_len;
  int64_t encode_pos = length_pos;
  // Remove the appended definition type and preserve the legacy UNIS object boundary.
  ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(
      buf, sizeof(buf), encode_pos, payload_len - def_type_len));

  // The missing definition type must preserve the initialized default.
  ObTableSchemaParam decoded_param(allocator);
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_param.deserialize(buf, legacy_len, read_pos));
  ASSERT_EQ(legacy_len, read_pos);
  ASSERT_EQ(param.get_table_id(), decoded_param.get_table_id());
  ASSERT_EQ(TABLE_DEF_TYPE_MAX, decoded_param.get_def_type());
}

static void init_column_schema(ObColumnSchemaV2 &column,
                               const uint64_t tenant_id,
                               const uint64_t table_id,
                               const uint64_t column_id,
                               const char *column_name,
                               const ObObjType data_type,
                               const int64_t rowkey_position)
{
  column.set_tenant_id(tenant_id);
  column.set_table_id(table_id);
  column.set_column_id(column_id);
  column.set_schema_version(1);
  ASSERT_EQ(OB_SUCCESS, column.set_column_name(column_name));
  column.set_data_type(data_type);
  column.set_data_length(static_cast<int64_t>(
      ObLongTextType == data_type ? 1024 : sizeof(int64_t)));
  column.set_rowkey_position(rowkey_position);
  column.set_order_in_rowkey(ObOrderType::ASC);
  column.set_nullable(0 == rowkey_position);
  column.set_charset_type(ObLongTextType == data_type ? CHARSET_UTF8MB4 : CHARSET_BINARY);
  column.set_collation_type(ObLongTextType == data_type ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_BINARY);
}

TEST(ObTableParam, construct_lob_locator_param_uses_pk_rowid)
{
  ObArenaAllocator allocator;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const uint64_t table_id = 3001;
  ObTableSchema table_schema;
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_schema_version(1);
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("t_lob_rowid"));
  table_schema.set_table_type(USER_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_table_organization_mode(TOM_INDEX_ORGANIZED);
  table_schema.set_table_pk_exists_mode(TOM_TABLE_WITH_PK);
  table_schema.set_charset_type(CHARSET_UTF8MB4);
  table_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  table_schema.set_max_used_column_id(19);

  ObColumnSchemaV2 pk1_schema;
  ObColumnSchemaV2 pk2_schema;
  ObColumnSchemaV2 lob_schema;
  ObColumnSchemaV2 normal_schema;
  init_column_schema(pk1_schema, tenant_id, table_id, 16, "pk1", ObIntType, 1);
  init_column_schema(pk2_schema, tenant_id, table_id, 17, "pk2", ObIntType, 2);
  init_column_schema(lob_schema, tenant_id, table_id, 18, "lob_col", ObLongTextType, 0);
  init_column_schema(normal_schema, tenant_id, table_id, 19, "normal_col", ObIntType, 0);
  ASSERT_EQ(OB_SUCCESS, table_schema.add_column(pk1_schema));
  ASSERT_EQ(OB_SUCCESS, table_schema.add_column(pk2_schema));
  ASSERT_EQ(OB_SUCCESS, table_schema.add_column(lob_schema));
  ASSERT_EQ(OB_SUCCESS, table_schema.add_column(normal_schema));
  ASSERT_TRUE(table_schema.is_valid());

  ObColumnParam pk1_param(allocator);
  ObColumnParam pk2_param(allocator);
  ObColumnParam lob_param(allocator);
  ObColumnParam normal_param(allocator);
  ASSERT_EQ(OB_SUCCESS, ObTableParam::convert_column_schema_to_param(pk1_schema, pk1_param));
  ASSERT_EQ(OB_SUCCESS, ObTableParam::convert_column_schema_to_param(pk2_schema, pk2_param));
  ASSERT_EQ(OB_SUCCESS, ObTableParam::convert_column_schema_to_param(lob_schema, lob_param));
  ASSERT_EQ(OB_SUCCESS, ObTableParam::convert_column_schema_to_param(normal_schema, normal_param));
  ObSEArray<ObColumnParam *, 4> storage_columns;
  ASSERT_EQ(OB_SUCCESS, storage_columns.push_back(&lob_param));
  ASSERT_EQ(OB_SUCCESS, storage_columns.push_back(&normal_param));
  ASSERT_EQ(OB_SUCCESS, storage_columns.push_back(&pk2_param));
  ASSERT_EQ(OB_SUCCESS, storage_columns.push_back(&pk1_param));

  Projector access_projector(allocator);
  Projector rowid_projector(allocator);
  ASSERT_EQ(OB_SUCCESS, access_projector.init(4));
  for (int32_t i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, access_projector.push_back(i));
  }
  bool use_lob_locator = false;
  int64_t rowid_version = ObURowIDData::INVALID_ROWID_VERSION;
  ObTableParam table_param(allocator);
  ASSERT_EQ(OB_SUCCESS, table_param.construct_lob_locator_param(
      table_schema, storage_columns, access_projector, use_lob_locator,
      rowid_version, rowid_projector, false));
  ASSERT_TRUE(use_lob_locator);
  ASSERT_EQ(ObURowIDData::PK_ROWID_VERSION, rowid_version);
  ASSERT_EQ(2, rowid_projector.count());
  ASSERT_EQ(3, rowid_projector.at(0));
  ASSERT_EQ(2, rowid_projector.at(1));
}

// Retain the encoding without projection fields to test TableParam compatibility in both directions.
class ObLegacyTableDMLParam final
{
  OB_UNIS_VERSION(1);
public:
  explicit ObLegacyTableDMLParam(ObIAllocator &allocator)
    : tenant_schema_version_(1), data_table_(allocator), col_descs_(allocator)
  {}
  ~ObLegacyTableDMLParam() = default;

  int64_t tenant_schema_version_;
  ObTableSchemaParam data_table_;
  ObTableDMLParam::ObColDescArray col_descs_;
};

OB_SERIALIZE_MEMBER(ObLegacyTableDMLParam, tenant_schema_version_, data_table_, col_descs_);

TEST(ObTableDMLParam, legacy_wire_compatibility)
{
  ObArenaAllocator allocator;
  ObTableDMLParam new_param(allocator);
  // Build serialization samples without schema lookups or projection calculation.
  new_param.tenant_schema_version_ = 1;
  new_param.data_table_.table_id_ = 50001;
  new_param.col_descs_.set_capacity(2);
  for (int64_t i = 0; i < 2; ++i) {
    ObColDesc column;
    column.col_id_ = OB_APP_MIN_COLUMN_ID + i;
    column.col_type_.set_int();
    ASSERT_EQ(OB_SUCCESS, new_param.col_descs_.push_back(column));
  }
  new_param.has_delete_old_row_log_projection_ = true;
  new_param.delete_old_row_log_column_idxs_.set_capacity(1);
  ASSERT_EQ(OB_SUCCESS, new_param.delete_old_row_log_column_idxs_.push_back(1));

  char new_buf[16384];
  char old_buf[16384];
  int64_t new_len = 0;
  int64_t old_len = 0;
  ASSERT_EQ(OB_SUCCESS, new_param.serialize(new_buf, sizeof(new_buf), new_len));

  // New writer, old reader: skip the new tail while preserving existing fields and object boundaries.
  ObLegacyTableDMLParam old_param(allocator);
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_param.deserialize(new_buf, new_len, read_pos));
  ASSERT_EQ(new_len, read_pos);
  ASSERT_EQ(new_param.tenant_schema_version_, old_param.tenant_schema_version_);
  ASSERT_EQ(new_param.get_data_table().get_table_id(), old_param.data_table_.get_table_id());
  ASSERT_EQ(2, old_param.col_descs_.count());
  ASSERT_EQ(OB_APP_MIN_COLUMN_ID + 1, old_param.col_descs_.at(1).col_id_);

  // Old writer, new reader: projection fields are absent, so the reader defaults to no pruning.
  ASSERT_EQ(OB_SUCCESS, old_param.serialize(old_buf, sizeof(old_buf), old_len));
  ObTableDMLParam decoded_param(allocator);
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_param.deserialize(old_buf, old_len, read_pos));
  ASSERT_EQ(old_len, read_pos);
  ASSERT_EQ(old_param.tenant_schema_version_, decoded_param.tenant_schema_version_);
  ASSERT_EQ(old_param.data_table_.get_table_id(), decoded_param.get_data_table().get_table_id());
  ASSERT_EQ(2, decoded_param.get_col_descs().count());
  ASSERT_EQ(nullptr, decoded_param.get_delete_old_row_log_column_idxs());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

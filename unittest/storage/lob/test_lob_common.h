/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "object/ob_obj_type.h"
#define protected public
#define private public
#include "test_dml_common.h"
#include "storage/test_tablet_helper.h"
#include "share/ob_tablet_autoincrement_service.h"

namespace oceanbase
{
namespace storage
{

class TestLobCommon
{
public:
  static int create_data_tablet(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &lob_meta_tablet_id,
      const common::ObTabletID &lob_piece_tablet_id);
  static void build_data_table_schema(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &table_schema);
  static void build_lob_meta_table_schema(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &table_schema);
  static void build_lob_piece_table_schema(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &table_schema);
  static const uint64_t TX_LOB_EXPIRE_TIME_US = 120 * 1000 * 1000; // 120s
  static const int64_t TEST_LOB_LS_ID = 2;
  static const uint64_t TEST_LOB_TABLE_ID = 50;
  static const uint64_t TEST_LOB_META_TABLE_ID = 51;
  static const uint64_t TEST_LOB_PIECE_TABLE_ID = 52;
};

int TestLobCommon::create_data_tablet(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObTabletID &lob_meta_tablet_id,
    const common::ObTabletID &lob_piece_tablet_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;

  if (OB_FAIL(TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle))) {
    STORAGE_LOG(WARN, "failed to create ls", K(ret), K(tenant_id), K(ls_id));
  } else {
    common::ObArenaAllocator allocator;
    { // create data tablet
      share::schema::ObTableSchema table_schema;
      build_data_table_schema(tenant_id, table_schema);
      if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator))) {
        STORAGE_LOG(WARN, "failed to create data tablet", K(ret), K(tablet_id));
      }
    }
    if (OB_SUCC(ret)) { // create lob meta tablet
      share::schema::ObTableSchema meta_schema;
      build_lob_meta_table_schema(tenant_id, meta_schema);
      if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, lob_meta_tablet_id, meta_schema, allocator))) {
        STORAGE_LOG(WARN, "failed to create lob meta tablet", K(ret), K(lob_meta_tablet_id));
      }
    }
    if (OB_SUCC(ret)) { // create lob piece tablet
      share::schema::ObTableSchema piece_schema;
      build_lob_piece_table_schema(tenant_id, piece_schema);
      if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, lob_piece_tablet_id, piece_schema, allocator))) {
        STORAGE_LOG(WARN, "failed to create lob piece tablet", K(ret), K(lob_piece_tablet_id));
      }
    }
    // 初始化 TabletAutoincrementService (必须 init 后才能 acquire_mgr)
    if (OB_SUCC(ret)) {
      share::ObTabletAutoincrementService &auto_inc = share::ObTabletAutoincrementService::get_instance();
      if (OB_FAIL(auto_inc.init())) {
        STORAGE_LOG(WARN, "failed to init auto increment service", K(ret));
      } else {
        share::ObTabletAutoincMgr *autoinc_mgr = nullptr;
        if (OB_FAIL(auto_inc.acquire_mgr(tenant_id, lob_meta_tablet_id,
            share::ObTabletAutoincrementService::LOB_CACHE_SIZE, autoinc_mgr))) {
          STORAGE_LOG(WARN, "failed to acquire autoinc mgr for lob meta tablet", K(ret), K(lob_meta_tablet_id));
        } else {
          // Set a valid cache interval so that set_interval() inside
          // fetch_interval() succeeds without calling fetch_new_range() (RPC).
          autoinc_mgr->curr_node_.cache_start_ = 1;
          autoinc_mgr->curr_node_.cache_end_ = UINT64_MAX;
          autoinc_mgr->next_value_ = 1;
          auto_inc.release_mgr(autoinc_mgr);
        }
      }
    }
  }

  return ret;
}

void TestLobCommon::build_data_table_schema(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &table_schema)
{
  const uint64_t table_id = TEST_LOB_TABLE_ID;
  const int64_t micro_block_size = 16 * 1024;

  table_schema.reset();
  table_schema.set_table_name("test_lob_common");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_schema_version(share::OB_CORE_SCHEMA_VERSION + 1);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_max_used_column_id(ObObjType::ObExtendType - 1);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ObRowStoreType::ENCODING_ROW_STORE);
  table_schema.set_storage_format_version(ObStorageFormatVersion::OB_STORAGE_FORMAT_VERSION_V4);
  table_schema.set_micro_index_clustered(false);

#define ADD_COLUMN(column_id, column_name, data_type, collation_type, is_row_key) \
  { \
    ObColumnSchemaV2 column; \
    column.set_tenant_id(tenant_id); \
    column.set_column_id(column_id); \
    column.set_column_name(column_name); \
    column.set_data_type(data_type); \
    column.set_collation_type(collation_type); \
    if (is_row_key) { \
      column.set_rowkey_position(1); \
    } \
    table_schema.add_column(column); \
  }

  // table schema
  // a(bigint)  b(bigint)  c(bigint)  d(longtext)  e(varchar)
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 0, "a", ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI, true);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 1, "b", ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI, false);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 2, "c", ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI, false);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 3, "d", ObLongTextType, CS_TYPE_UTF8MB4_BIN, false);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 4, "e", ObVarcharType, CS_TYPE_UTF8MB4_BIN, false);
#undef ADD_COLUMN
}

void TestLobCommon::build_lob_meta_table_schema(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &table_schema)
{
  const uint64_t data_table_id = TEST_LOB_TABLE_ID;
  const uint64_t lob_meta_table_id = TEST_LOB_META_TABLE_ID;
  const int64_t micro_block_size = 16 * 1024;

  // align with ObLobMetaUtil: 3 rowkey + 4 normal = 7 columns
  static const uint64_t META_COLUMN_CNT = 7;

  table_schema.reset();
  table_schema.set_table_name("test_lob_meta_common");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_data_table_id(data_table_id);
  table_schema.set_table_id(lob_meta_table_id);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_schema_version(share::OB_CORE_SCHEMA_VERSION + 1);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_max_used_column_id(ObObjType::ObExtendType - 1);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ObRowStoreType::ENCODING_ROW_STORE);
  table_schema.set_storage_format_version(ObStorageFormatVersion::OB_STORAGE_FORMAT_VERSION_V4);
  table_schema.set_micro_index_clustered(false);

  // rowkey col 0: lob_col_idx (int64)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 0);
    col.set_column_name("lob_col_idx");
    col.set_rowkey_position(1);
    col.set_data_type(common::ObIntType);
    col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    table_schema.add_column(col);
  }
  // rowkey col 1: lob_id (varbinary:16)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 1);
    col.set_column_name("lob_id");
    col.set_rowkey_position(2);
    col.set_data_type(ObVarcharType);
    col.set_collation_type(CS_TYPE_BINARY);
    col.set_data_length(static_cast<int32_t>(sizeof(ObLobId)));
    table_schema.add_column(col);
  }
  // rowkey col 2: seq_id (varbinary:512)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 2);
    col.set_column_name("seq_id");
    col.set_rowkey_position(3);
    col.set_data_type(ObVarcharType);
    col.set_collation_type(CS_TYPE_BINARY);
    col.set_data_length(8192);
    table_schema.add_column(col);
  }
  // normal col 0: byte_len (uint32)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 3);
    col.set_column_name("byte_len");
    col.set_data_type(ObUInt32Type);
    col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    table_schema.add_column(col);
  }
  // normal col 1: char_len (uint32)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 4);
    col.set_column_name("char_len");
    col.set_data_type(ObUInt32Type);
    col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    table_schema.add_column(col);
  }
  // normal col 2: lob_info (varbinary:16384)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 5);
    col.set_column_name("lob_info");
    col.set_data_type(ObVarcharType);
    col.set_collation_type(CS_TYPE_BINARY);
    col.set_data_length(16384);
    table_schema.add_column(col);
  }
  // normal col 3: lob_block_id (varbinary:128)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 6);
    col.set_column_name("lob_block_id");
    col.set_data_type(ObVarcharType);
    col.set_collation_type(CS_TYPE_BINARY);
    col.set_data_length(128);
    table_schema.add_column(col);
  }
}

void TestLobCommon::build_lob_piece_table_schema(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &table_schema)
{
  const uint64_t data_table_id = TEST_LOB_TABLE_ID;
  const uint64_t lob_piece_table_id = TEST_LOB_PIECE_TABLE_ID;
  const int64_t micro_block_size = 16 * 1024;

  // align with ObLobPieceUtil: 3 rowkey + 1 normal = 4 columns
  static const uint64_t PIECE_COLUMN_CNT = 4;

  table_schema.reset();
  table_schema.set_table_name("test_lob_piece_common");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_data_table_id(data_table_id);
  table_schema.set_table_id(lob_piece_table_id);
  table_schema.set_table_type(AUX_LOB_PIECE);
  table_schema.set_schema_version(share::OB_CORE_SCHEMA_VERSION + 1);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_max_used_column_id(ObObjType::ObExtendType - 1);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ObRowStoreType::ENCODING_ROW_STORE);
  table_schema.set_storage_format_version(ObStorageFormatVersion::OB_STORAGE_FORMAT_VERSION_V4);
  table_schema.set_micro_index_clustered(false);

  // rowkey col 0: lob_col_idx (int64)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 0);
    col.set_column_name("lob_col_idx");
    col.set_rowkey_position(1);
    col.set_data_type(ObIntType);
    col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    table_schema.add_column(col);
  }
  // rowkey col 1: lob_id (varbinary:16)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 1);
    col.set_column_name("lob_id");
    col.set_rowkey_position(2);
    col.set_data_type(ObVarcharType);
    col.set_collation_type(CS_TYPE_BINARY);
    col.set_data_length(static_cast<int32_t>(sizeof(ObLobId)));
    table_schema.add_column(col);
  }
  // rowkey col 2: seq_id (varbinary:512)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 2);
    col.set_column_name("seq_id");
    col.set_rowkey_position(3);
    col.set_data_type(ObVarcharType);
    col.set_collation_type(CS_TYPE_BINARY);
    col.set_data_length(8192);
    table_schema.add_column(col);
  }
  // normal col 0: lob_data (varbinary:262144)
  {
    ObColumnSchemaV2 col;
    col.set_tenant_id(tenant_id);
    col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 3);
    col.set_column_name("lob_data");
    col.set_data_type(ObVarcharType);
    col.set_collation_type(CS_TYPE_BINARY);
    col.set_data_length(262144);
    table_schema.add_column(col);
  }
}


} // storage
} // oceanbase

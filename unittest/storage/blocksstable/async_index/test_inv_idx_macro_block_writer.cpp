/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include <atomic>
#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/search_index/ob_search_index_generator.h"
#include "share/search_index/ob_search_index_config.h"
#include "unittest/share/search_index/search_index_config_test_util.h"
#include "share/ob_fts_pos_list_codec.h"
#include "plugin/legacy/sys/ob_plugin_mgr.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_base.h"
#include <algorithm>
#include <thread>

#define private public
#define protected public

#include "lib/allocator/page_arena.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_schema.h"
#include "share/search_index/ob_search_index_nested_key_list.h"
#include "src/share/schema/ob_table_param.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_decoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_writer.h"
#include "storage/blocksstable/async_index/ob_inv_idx_index_block_builder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_transformer.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_helper.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/ddl/ob_async_index_macro_writer.h"
#include "unittest/storage/blocksstable/cs_encoding/ob_row_vector_converter.h"

namespace oceanbase
{
namespace blocksstable
{

// =============================================================================
// Helper: build a minimal inverted-index table schema with FTS column flags.
//
// Column layout (order matters — matches storage_datums_ in ObDatumRow):
//   col 0: term (VARCHAR, rowkey)
//   col 1: doc_id (uint64, GENERATED_DOC_ID_COLUMN_FLAG)
//   col 2: token_freq (uint64, GENERATED_FTS_WORD_COUNT_COLUMN_FLAG)
//   col 3: doc_length (uint64, GENERATED_FTS_DOC_LENGTH_COLUMN_FLAG)
//   col 4: pos_list (VARCHAR, GENERATED_FTS_POS_LIST_COLUMN_FLAG)
// =============================================================================
static void prepare_inv_idx_schema(
    share::schema::ObTableSchema &table_schema,
    const bool has_pos_list = true,
    const bool has_relevance = true,
    const int64_t micro_block_size = 16 * 1024,
    const char *compress_name = "none")
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = 1;
  const uint64_t table_id = 7777;
  const int64_t rowkey_column_cnt = 2;   // term, doc_id
  const int64_t column_cnt = 2 + (has_relevance ? 2 : 0) + (has_pos_list ? 1 : 0);

  table_schema.reset();
  ret = table_schema.set_table_name("test_inv_idx");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_column_cnt);
  table_schema.set_max_used_column_id(common::OB_APP_MIN_COLUMN_ID + column_cnt);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name(compress_name);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_pctfree(10);

  // Column metadata: (rowkey_pos, obj_type, flag). rowkey is (term, doc_id).
  struct ColMeta
  {
    int64_t rowkey_pos_;   // 0 = non-rowkey, >=1 = rowkey position
    ObObjType obj_type_;
    int64_t flags_;
  };
  // term, doc_id always; token_freq + doc_length when has_relevance; pos_list when has_pos_list.
  ColMeta col_metas[5];
  int64_t meta_cnt = 0;
  col_metas[meta_cnt++] = {1, ObVarcharType, 0};                                   // term     (rowkey #1)
  col_metas[meta_cnt++] = {2, ObUInt64Type,  GENERATED_DOC_ID_COLUMN_FLAG};        // doc_id   (rowkey #2)
  if (has_relevance) {
    col_metas[meta_cnt++] = {0, ObUInt64Type,  GENERATED_FTS_WORD_COUNT_COLUMN_FLAG};  // token_freq
    col_metas[meta_cnt++] = {0, ObUInt64Type,  GENERATED_FTS_DOC_LENGTH_COLUMN_FLAG};  // doc_length
  }
  if (has_pos_list) {
    col_metas[meta_cnt++] = {0, ObVarcharType, GENERATED_FTS_POS_LIST_COLUMN_FLAG};    // pos_list
  }

  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < column_cnt; ++i) {
    share::schema::ObColumnSchemaV2 column;
    const int64_t column_id = common::OB_APP_MIN_COLUMN_ID + i;
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(col_metas[i].obj_type_);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    column.set_rowkey_position(col_metas[i].rowkey_pos_);
    if (0 != col_metas[i].flags_) {
      column.set_column_flags(col_metas[i].flags_);
    }

    share::schema::ObSkipIndexColumnAttr skip_idx_attr;
    if (!is_lob_storage(col_metas[i].obj_type_)) {
      skip_idx_attr.set_min_max();
      column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    }
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
}

// =============================================================================
// Helper: prepare a valid ObInvIdxRowDesc from the test schema.
// In the test environment, index_schema and data_schema are the same object
// (flags are present on both), so we pass table_schema for both parameters.
// =============================================================================
static int prepare_inv_idx_row_desc(
    const share::schema::ObTableSchema &table_schema,
    ObInvIdxRowDesc &row_desc)
{
  return row_desc.init(table_schema, table_schema);
}

// =============================================================================
// Helper: build an ObDatumRow in the multi-version column layout.
//
// get_multi_version_column_descs inserts MV-extra rowkey columns (trans_version,
// sql_sequence) right after the schema rowkey columns, so inv_idx_row_desc indices
// are relative to that MV layout. The caller supplies mv_col_cnt (= row_column_count_)
// so the row has the right column count.
// =============================================================================
static int make_inv_idx_row(
    ObDatumRow &row,
    const ObInvIdxRowDesc &row_desc,
    const int64_t mv_col_cnt,
    const char *term_str,
    const uint64_t doc_id,
    const uint64_t token_freq,
    const uint64_t doc_length,
    const char *pos_list_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(mv_col_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid mv col cnt", K(ret), K(mv_col_cnt));
  } else if (OB_UNLIKELY(!row.is_valid() || row.get_column_count() != mv_col_cnt)) {
    row.reset();
    if (OB_FAIL(row.init(mv_col_cnt))) {
      STORAGE_LOG(WARN, "fail to init datum row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    row.count_ = static_cast<uint16_t>(mv_col_cnt);
    // Fill every column with int 0 first: this clears the ext flag (set_int sets
    // pack_=8 -> flag_=NONE) so ObRowVectorConverter, which rejects is_ext() datums,
    // can ingest the row, and reuse() repoints ptr_ at the inline buf_ so set_int's
    // indirect write is safe even after a prior set_string on a reused row. The real
    // columns are overwritten below; tf/dl/pos_list are optional (absent -> col_idx_ < 0).
    for (int16_t i = 0; i < row.count_; ++i) {
      row.storage_datums_[i].reuse();
      row.storage_datums_[i].set_int(0);
    }
    row.storage_datums_[0].set_string(ObString(term_str));
    row.storage_datums_[row_desc.doc_id_col_idx_].set_uint(doc_id);
    if (row_desc.token_freq_col_idx_ >= 0) {
      row.storage_datums_[row_desc.token_freq_col_idx_].set_uint(token_freq);
    }
    if (row_desc.doc_length_col_idx_ >= 0) {
      row.storage_datums_[row_desc.doc_length_col_idx_].set_uint(doc_length);
    }
    if (row_desc.pos_list_col_idx_ >= 0) {
      row.storage_datums_[row_desc.pos_list_col_idx_].set_string(ObString(pos_list_str));
    }
  }
  return ret;
}

// =============================================================================
// Helper: RAII guard that temporarily raises GCONF.micro_block_merge_verify_level so each
// compressed data micro block is round-trip verified (decompress + compare) on the write path,
// then restores the original value — even if the test body fails an ASSERT and returns early.
// =============================================================================
struct VerifyLevelGuard
{
  explicit VerifyLevelGuard(const int64_t level)
    : saved_(GCONF.micro_block_merge_verify_level)
  {
    GCONF.micro_block_merge_verify_level = level;
  }
  ~VerifyLevelGuard() { GCONF.micro_block_merge_verify_level = saved_; }
  int64_t saved_;
};

// =============================================================================
// Helper: append enough rows of one term to flush at least one data micro block mid-stream,
// then a new term to close the pending group. Both paths prepare the candidate pair through
// ObInvIdxMicroBlockBufferHelper, exercising compression and write-path verification.
// =============================================================================
static int append_compress_round_trip_rows(
    ObInvIdxMacroBlockWriter &writer,
    const ObInvIdxRowDesc &row_desc,
    const int64_t mv_col_cnt)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  const int64_t row_cnt = 8000;
  const uint64_t big_val = 1000000;  // forces multi-byte packing in the posting block
  for (int64_t i = 1; OB_SUCC(ret) && i <= row_cnt; ++i) {
    if (OB_FAIL(make_inv_idx_row(row, row_desc, mv_col_cnt,
            "a", static_cast<uint64_t>(i), big_val, big_val, "p"))) {
      STORAGE_LOG(WARN, "fail to make inv idx row", K(ret));
    } else if (OB_FAIL(writer.append_row(row))) {
      STORAGE_LOG(WARN, "fail to append row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(make_inv_idx_row(row, row_desc, mv_col_cnt,
            "b", static_cast<uint64_t>(row_cnt + 1), big_val, big_val, "p"))) {
      STORAGE_LOG(WARN, "fail to make inv idx row", K(ret));
    } else if (OB_FAIL(writer.append_row(row))) {
      STORAGE_LOG(WARN, "fail to append row", K(ret));
    }
  }
  return ret;
}

// =============================================================================
// Helper: build a search-index table schema.
//
// Column layout (matches storage_datums_ in ObDatumRow):
//   col 0: COL_IDX  (VARCHAR, rowkey #1, term col)
//   col 1: PATH     (VARCHAR, rowkey #2, term col)
//   col 2: VALUE    (VARCHAR, rowkey #3, term col)
//   col 3: DOC_ID   (uint64,  rowkey #4, GENERATED_DOC_ID_COLUMN_FLAG)
//   col 4: NESTED_KEY (VARCHAR, non-rowkey, no flag — schema layer has no nested_key flag yet)
//
// rowkey_column_cnt = 4, so term_col_count = 3 (schema rowkey minus trailing doc_id).
// =============================================================================
static void prepare_search_idx_schema(
    share::schema::ObTableSchema &table_schema,
    const int64_t micro_block_size = 16 * 1024,
    const char *compress_name = "none")
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = 1;
  const uint64_t table_id = 9999;
  const int64_t rowkey_column_cnt = 4;
  const int64_t column_cnt = 5;

  table_schema.reset();
  ret = table_schema.set_table_name("test_search_idx");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_column_cnt);
  table_schema.set_max_used_column_id(common::OB_APP_MIN_COLUMN_ID + column_cnt);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name(compress_name);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_pctfree(10);

  struct ColMeta { int64_t rowkey_pos_; ObObjType obj_type_; int64_t flags_; };
  ColMeta col_metas[5];
  col_metas[0] = {1, ObVarcharType, 0};                            // COL_IDX
  col_metas[1] = {2, ObVarcharType, 0};                            // PATH
  col_metas[2] = {3, ObVarcharType, 0};                            // VALUE
  col_metas[3] = {4, ObUInt64Type, GENERATED_DOC_ID_COLUMN_FLAG}; // DOC_ID
  col_metas[4] = {0, ObVarcharType, 0};                            // NESTED_KEY

  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < column_cnt; ++i) {
    share::schema::ObColumnSchemaV2 column;
    const int64_t column_id = common::OB_APP_MIN_COLUMN_ID + i;
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "search%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(col_metas[i].obj_type_);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    column.set_rowkey_position(col_metas[i].rowkey_pos_);
    if (0 != col_metas[i].flags_) {
      column.set_column_flags(col_metas[i].flags_);
    }
    share::schema::ObSkipIndexColumnAttr skip_idx_attr;
    if (!is_lob_storage(col_metas[i].obj_type_)) {
      skip_idx_attr.set_min_max();
      column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    }
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
}

// =============================================================================
// Helper: build a search-index ObDatumRow in the multi-version column layout.
//
// MV layout: COL_IDX, PATH, VALUE, DOC_ID, trans_version, sql_sequence, NESTED_KEY
// term_col_cnt = 3 (first three datums are the term prefix).
// =============================================================================
static int make_search_idx_row_with_nested_key(
    ObDatumRow &row,
    const ObInvIdxRowDesc &row_desc,
    const int64_t mv_col_cnt,
    const char *col_idx_str,
    const char *path_str,
    const char *value_str,
    const uint64_t doc_id,
    const ObDatum &nested_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(mv_col_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid mv col cnt", K(ret), K(mv_col_cnt));
  } else if (OB_UNLIKELY(!row.is_valid() || row.get_column_count() != mv_col_cnt)) {
    row.reset();
    if (OB_FAIL(row.init(mv_col_cnt))) {
      STORAGE_LOG(WARN, "fail to init datum row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    row.count_ = static_cast<uint16_t>(mv_col_cnt);
    for (int16_t i = 0; i < row.count_; ++i) {
      row.storage_datums_[i].reuse();
      row.storage_datums_[i].set_int(0);
    }
    row.storage_datums_[0].set_string(ObString(col_idx_str));
    row.storage_datums_[1].set_string(ObString(path_str));
    row.storage_datums_[2].set_string(ObString(value_str));
    row.storage_datums_[row_desc.doc_id_col_idx_].set_uint(doc_id);
    if (row_desc.nested_key_col_idx_ >= 0) {
      row.storage_datums_[row_desc.nested_key_col_idx_].shallow_copy_from_datum(nested_key);
    }
  }
  return ret;
}

static int make_search_idx_row(
    ObDatumRow &row,
    const ObInvIdxRowDesc &row_desc,
    const int64_t mv_col_cnt,
    const char *col_idx_str,
    const char *path_str,
    const char *value_str,
    const uint64_t doc_id,
    const char *nested_key_str)
{
  ObDatum nested_key;
  nested_key.set_string(ObString(nested_key_str));
  return make_search_idx_row_with_nested_key(
      row, row_desc, mv_col_cnt, col_idx_str, path_str, value_str, doc_id, nested_key);
}

// =============================================================================
// Helper: build a search text index table schema.
//
// Logical column layout (storage rows add MVCC columns after the rowkey):
//   col 0: COL_IDX  (uint64,    rowkey #1, term col)
//   col 1: PATH     (VARBINARY, rowkey #2, term col)
//   col 2: TOKEN    (VARBINARY, rowkey #3, term col)
//   col 3: DOC_ID   (uint64,    rowkey #4)
//   col 4: TOKEN_FREQ (uint64)
//   col 5: DOC_LENGTH (uint64)
//   col 6: POSITION_LIST (VARBINARY), PHRASE only
// =============================================================================
static void prepare_search_text_idx_schema(
    share::schema::ObTableSchema &table_schema,
    const bool has_pos_list,
    const int64_t micro_block_size = 16 * 1024)
{
  const uint64_t table_id = 8888;
  const int64_t rowkey_column_cnt = SEARCH_TEXT_ROWKEY_CNT;
  const int64_t column_cnt = has_pos_list ? SEARCH_TEXT_COL_CNT_WITH_POS
                                          : SEARCH_TEXT_COL_CNT_WITHOUT_POS;

  table_schema.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("test_search_text_idx"));
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_index_type(has_pos_list
      ? share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL
      : share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL);
  // index_type implies an index table; reset() leaves USER_TABLE which fails
  // ObSimpleTableSchemaV2::is_valid() (table_type/index_type mismatch, then data_table_id).
  table_schema.set_table_type(share::schema::USER_INDEX);
  table_schema.set_data_table_id(9999);
  table_schema.set_rowkey_column_num(rowkey_column_cnt);
  table_schema.set_max_used_column_id(common::OB_APP_MIN_COLUMN_ID + column_cnt);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_pctfree(10);

  struct ColMeta { int64_t rowkey_pos_; ObObjType obj_type_; };
  const ColMeta col_metas[SEARCH_TEXT_COL_CNT_WITH_POS] = {
      {1, ObUInt64Type},    // COL_IDX
      {2, ObVarcharType},   // PATH
      {3, ObVarcharType},   // TOKEN
      {4, ObUInt64Type},    // DOC_ID
      {0, ObUInt64Type},    // TOKEN_FREQ
      {0, ObUInt64Type},    // DOC_LENGTH
      {0, ObVarcharType}};  // POSITION_LIST

  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < column_cnt; ++i) {
    share::schema::ObColumnSchemaV2 column;
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(common::OB_APP_MIN_COLUMN_ID + i);
    sprintf(name, "search_text%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(col_metas[i].obj_type_);
    column.set_collation_type(CS_TYPE_BINARY);
    if (i == SEARCH_TEXT_TOKEN) { column.set_charset_type(CHARSET_BINARY); }
    column.set_data_length(64);
    column.set_nullable(false);
    column.set_rowkey_position(col_metas[i].rowkey_pos_);
    share::schema::ObSkipIndexColumnAttr skip_idx_attr;
    skip_idx_attr.set_min_max();
    column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
}

// =============================================================================
// Helper: build a search text ObDatumRow in the multi-version column layout.
//
// MV layout: COL_IDX, PATH, TOKEN, DOC_ID, trans_version, sql_sequence, TF, DL, [POS]
// =============================================================================
static int make_search_text_row(
    ObDatumRow &row,
    const ObInvIdxRowDesc &row_desc,
    const int64_t mv_col_cnt,
    const uint64_t col_idx,
    const char *path_str,
    const char *token_str,
    const uint64_t doc_id,
    const uint64_t token_freq,
    const uint64_t doc_length,
    const char *pos_list_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(mv_col_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid mv col cnt", K(ret), K(mv_col_cnt));
  } else if (OB_UNLIKELY(!row.is_valid() || row.get_column_count() != mv_col_cnt)) {
    row.reset();
    if (OB_FAIL(row.init(mv_col_cnt))) {
      STORAGE_LOG(WARN, "fail to init datum row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    row.count_ = static_cast<uint16_t>(mv_col_cnt);
    for (int16_t i = 0; i < row.count_; ++i) {
      row.storage_datums_[i].reuse();
      row.storage_datums_[i].set_int(0);
    }
    row.storage_datums_[SEARCH_TEXT_COL_IDX].set_uint(col_idx);
    row.storage_datums_[SEARCH_TEXT_PATH].set_string(ObString(path_str));
    row.storage_datums_[SEARCH_TEXT_TOKEN].set_string(ObString(token_str));
    row.storage_datums_[row_desc.doc_id_col_idx_].set_uint(doc_id);
    row.storage_datums_[row_desc.token_freq_col_idx_].set_uint(token_freq);
    row.storage_datums_[row_desc.doc_length_col_idx_].set_uint(doc_length);
    if (row_desc.pos_list_col_idx_ >= 0) {
      row.storage_datums_[row_desc.pos_list_col_idx_].set_string(ObString(pos_list_str));
    }
  }
  return ret;
}

// =============================================================================
// ObInvIdxMacroBlockWriterTest
// =============================================================================
class ObInvIdxMacroBlockWriterTest : public ::testing::Test
{
protected:
  ObInvIdxMacroBlockWriterTest()
    : mock_ls_id_(1),
      mock_tablet_id_(1),
      sstable_builder_(false /*use_double_write_buffer*/),
      mv_col_cnt_(0)
  {}

  virtual ~ObInvIdxMacroBlockWriterTest() {}

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  virtual void SetUp()
  {
    // Reuse MockTenantModuleEnv's sys tenant (500) context — do NOT override it
    // with a bare local ObTenantBase, which loses the IO manager and makes close()
    // fail with OB_HASH_NOT_EXIST (-4201).
    allocator_.reset();
    row_allocator_.reset();
    table_schema_.reset();
    static_desc_.reset();
    col_desc_.reset();
    data_store_desc_.reset();
    inv_idx_row_desc_.reset();
    writer_.reset();
    sstable_builder_.reset();
    object_cleaner_.reset();
    macro_seq_param_.reset();
    mv_col_cnt_ = 0;
  }

  virtual void TearDown()
  {
    writer_.reset();
    sstable_builder_.reset();
    object_cleaner_.reset();
    data_store_desc_.reset();
    col_desc_.reset();
    static_desc_.reset();
    table_schema_.reset();
    row_allocator_.reset();
    allocator_.reset();
  }

  static ObInvSSTableMeta make_inv_meta(
      const uint64_t min_doc_id = UINT64_MAX,
      const uint64_t max_doc_id = 0,
      const uint64_t total_doc_length = 0)
  {
    ObInvIdxStatsMeta stats;
    stats.index_type_ = INV_IDX_TYPE_FTS;
    stats.has_relevance_params_ = true;
    stats.has_position_list_ = true;
    stats.doc_id_obj_meta_.set_type(ObUInt64Type);
    stats.min_doc_id_ = min_doc_id;
    stats.max_doc_id_ = max_doc_id;
    stats.total_doc_length_ = total_doc_length;
    ObInvSSTableMeta meta;
    EXPECT_EQ(OB_SUCCESS, meta.init(stats));
    return meta;
  }

  // Set up a minimal but valid environment ready for writer.open().
  // has_pos_list=false builds a schema without pos_list (has_posting_value()==false).
  // has_relevance=false further drops token_freq/doc_length (no BM25, doc_id-only posting).
  // compress_name selects the table compressor (default "none"); real compressors exercise the
  // data micro block compression path in ObInvIdxMicroBlockBufferHelper.
  // micro_index_clustered=true builds the macro index as a clustered index tree, so
  // append_macro_block requires the reused macro's clustered index micro block.
  int prepare_open_env(const bool has_pos_list = true, const bool has_relevance = true,
                       const char *compress_name = "none",
                       const compaction::ObMergeType merge_type = compaction::MINI_MERGE,
                       const bool micro_index_clustered = false);
  // Variant of prepare_open_env for search index: builds a multi-col-term schema
  // (COL_IDX, PATH, VALUE, DOC_ID, NESTED_KEY) and manually sets index_type_ = SEARCH
  // and nested_key_col_idx_ after init(), simulating a correctly filled row desc
  // (schema layer has no is_async_search_index / nested_key flag yet).
  int prepare_search_open_env(const char *compress_name = "none");
  // Variant of prepare_open_env for search text index: builds the COL_IDX/PATH/TOKEN/DOC_ID
  // rowkey schema with MATCH/PHRASE index types, so the row desc comes from the
  // production init_search_text() path.
  int prepare_search_text_open_env(const bool has_pos_list);
  // Steps shared by every open env after the schema and row desc are ready: data store desc,
  // sstable index builder and macro sequence param.
  int prepare_store_env(const compaction::ObMergeType merge_type, const bool micro_index_clustered);
  // Open a writer against the env prepared by prepare_open_env(). Reused by the
  // append_row normal-path cases to avoid repeating the open() boilerplate.
  int open_writer(ObInvIdxMacroBlockWriter &writer, ObSSTablePrivateObjectCleaner &cleaner);
  // Build a reusable macro block with a separate writer. The donor writer must remain alive while
  // the returned meta is reused because it owns the macro block reference.
  int build_reusable_macro_meta(
      ObSSTableIndexBuilder &donor_builder,
      ObInvIdxMacroBlockWriter &donor_writer,
      ObSSTablePrivateObjectCleaner &donor_cleaner,
      common::ObIAllocator &alloc,
      ObDataMacroBlockMeta *&out_meta);
  static void make_reuse_desc(ObDataMacroBlockMeta &meta, ObMacroBlockDesc &desc);

protected:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator row_allocator_;
  share::schema::ObTableSchema table_schema_;
  ObStaticDataStoreDesc static_desc_;
  ObColDataStoreDesc col_desc_;
  ObDataStoreDesc data_store_desc_;
  ObInvIdxRowDesc inv_idx_row_desc_;
  ObInvIdxMacroBlockWriter writer_;
  ObSSTablePrivateObjectCleaner object_cleaner_;
  ObMacroSeqParam macro_seq_param_;
  share::ObLSID mock_ls_id_;
  ObTabletID mock_tablet_id_;
  ObSSTableIndexBuilder sstable_builder_;
  int64_t mv_col_cnt_;   // total columns in multi-version layout (= row_column_count_)
};

int ObInvIdxMacroBlockWriterTest::prepare_open_env(const bool has_pos_list, const bool has_relevance,
                                                   const char *compress_name,
                                                   const compaction::ObMergeType merge_type,
                                                   const bool micro_index_clustered)
{
  int ret = OB_SUCCESS;

  // 1. Prepare schema with FTS flags
  prepare_inv_idx_schema(table_schema_, has_pos_list, has_relevance, 16 * 1024, compress_name);
  table_schema_.set_micro_index_clustered(micro_index_clustered);
  if (OB_FAIL(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_))) {
    STORAGE_LOG(WARN, "fail to prepare inv idx row desc", K(ret));
  } else {
    inv_idx_row_desc_.index_type_ =
        has_relevance ? INV_IDX_TYPE_FTS : INV_IDX_TYPE_SEARCH;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_store_env(merge_type, micro_index_clustered))) {
    STORAGE_LOG(WARN, "fail to prepare store env", K(ret));
  }
  return ret;
}

int ObInvIdxMacroBlockWriterTest::prepare_search_text_open_env(const bool has_pos_list)
{
  int ret = OB_SUCCESS;
  prepare_search_text_idx_schema(table_schema_, has_pos_list);
  if (OB_FAIL(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_))) {
    STORAGE_LOG(WARN, "fail to prepare search text row desc", K(ret));
  } else if (OB_UNLIKELY(INV_IDX_TYPE_SEARCH_TEXT != inv_idx_row_desc_.index_type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected search text row desc", K(ret), K(inv_idx_row_desc_));
  } else if (OB_FAIL(prepare_store_env(compaction::MINI_MERGE, false /*micro_index_clustered*/))) {
    STORAGE_LOG(WARN, "fail to prepare store env", K(ret));
  }
  return ret;
}

int ObInvIdxMacroBlockWriterTest::prepare_store_env(
    const compaction::ObMergeType merge_type,
    const bool micro_index_clustered)
{
  int ret = OB_SUCCESS;

  // 2. Init ObStaticDataStoreDesc
  {
    share::SCN end_scn;
    share::SCN reorganization_scn;
    const int64_t cluster_version = DATA_VERSION_4_6_0_0;
    end_scn.convert_for_tx(100);
    reorganization_scn.set_min();
    if (OB_FAIL(static_desc_.init(
        false /*is_ddl*/,
        table_schema_,
        mock_ls_id_,
        mock_tablet_id_,
        0 /*private_transfer_epoch*/,
        merge_type,
        10000 /*snapshot_version*/,
        end_scn,
        cluster_version,
        compaction::EXEC_MODE_LOCAL,
        micro_index_clustered,
        0 /*concurrent_cnt*/,
        reorganization_scn))) {
      STORAGE_LOG(WARN, "fail to init static data store desc", K(ret));
    }
  }

  // 3. Init ObColDataStoreDesc — we need to set up column descriptors for the schema
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(col_desc_.init(table_schema_, 0 /*table_cg_idx*/, static_desc_))) {
    STORAGE_LOG(WARN, "fail to init col data store desc", K(ret));
  } else {
    // total columns in the multi-version layout (= 5 schema + 2 MV extra rowkey = 7)
    mv_col_cnt_ = col_desc_.row_column_count_;
  }

  // 4. Init ObDataStoreDesc
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data_store_desc_.init(static_desc_, col_desc_, table_schema_, FLAT_ROW_STORE))) {
    STORAGE_LOG(WARN, "fail to init data store desc", K(ret));
  } else {
    // data_store_type_ must be set explicitly since reset()+constructor don't;
    // SSTableData is the correct type for the inverted-index data macro block.
    data_store_desc_.data_store_type_ = ObMacroBlockCommonHeader::SSTableData;
  }

  // 5. Init ObSSTableIndexBuilder and attach to data_store_desc_
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sstable_builder_.init(data_store_desc_))) {
    STORAGE_LOG(WARN, "fail to init sstable index builder", K(ret));
  } else {
    data_store_desc_.sstable_index_builder_ = &sstable_builder_;
  }

  // 6. Init ObMacroSeqParam (incremental sequence)
  if (OB_FAIL(ret)) {
  } else {
    macro_seq_param_.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param_.start_ = 0;
    macro_seq_param_.interval_ = 1;
    macro_seq_param_.step_ = 1;
  }

  return ret;
}

int ObInvIdxMacroBlockWriterTest::open_writer(
    ObInvIdxMacroBlockWriter &writer,
    ObSSTablePrivateObjectCleaner &cleaner)
{
  share::ObPreWarmerParam pre_warm_param(share::PRE_WARM_TYPE_NONE);
  return writer.open(inv_idx_row_desc_, data_store_desc_, 0 /*parallel_idx*/,
      macro_seq_param_, pre_warm_param, cleaner, nullptr /*callback*/);
}

int ObInvIdxMacroBlockWriterTest::prepare_search_open_env(const char *compress_name)
{
  int ret = OB_SUCCESS;

  // 1. Build search index schema (multi-col term + doc_id + nested_key)
  prepare_search_idx_schema(table_schema_, 16 * 1024, compress_name);

  // 2. Init row desc from schema (resolves doc_id_col_idx_ from flag;
  //    token_freq/doc_length/pos_list stay -1). Manually set index_type_ = SEARCH
  //    since schema layer has no is_async_search_index() yet.
  if (OB_FAIL(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_))) {
    STORAGE_LOG(WARN, "fail to prepare search idx row desc", K(ret));
  } else {
    inv_idx_row_desc_.index_type_ = INV_IDX_TYPE_SEARCH;
  }

  // 3. Init ObStaticDataStoreDesc
  if (OB_FAIL(ret)) {
  } else {
    share::SCN end_scn;
    share::SCN reorganization_scn;
    const int64_t cluster_version = DATA_VERSION_4_6_0_0;
    end_scn.convert_for_tx(100);
    reorganization_scn.set_min();
    if (OB_FAIL(static_desc_.init(
        false /*is_ddl*/,
        table_schema_,
        mock_ls_id_,
        mock_tablet_id_,
        0 /*private_transfer_epoch*/,
        compaction::MINI_MERGE,
        10000 /*snapshot_version*/,
        end_scn,
        cluster_version,
        compaction::EXEC_MODE_LOCAL,
        false /*micro_index_clustered*/,
        0 /*concurrent_cnt*/,
        reorganization_scn))) {
      STORAGE_LOG(WARN, "fail to init static data store desc", K(ret));
    }
  }

  // 4. Init ObColDataStoreDesc
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(col_desc_.init(table_schema_, 0 /*table_cg_idx*/, static_desc_))) {
    STORAGE_LOG(WARN, "fail to init col data store desc", K(ret));
  } else {
    mv_col_cnt_ = col_desc_.row_column_count_;
  }

  // 5. Init ObDataStoreDesc
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data_store_desc_.init(static_desc_, col_desc_, table_schema_, FLAT_ROW_STORE))) {
    STORAGE_LOG(WARN, "fail to init data store desc", K(ret));
  } else {
    data_store_desc_.data_store_type_ = ObMacroBlockCommonHeader::SSTableData;
  }

  // 6. Resolve nested_key_col_idx_ from the MV column layout. NESTED_KEY
  //    (col_id = OB_APP_MIN_COLUMN_ID + 4) is the only non-rowkey schema column;
  //    schema layer has no nested_key flag, so find it by col_id and set manually.
  if (OB_FAIL(ret)) {
  } else {
    const common::ObIArray<share::schema::ObColDesc> &col_descs =
        data_store_desc_.get_full_stored_col_descs();
    bool found = false;
    for (int64_t i = 0; i < col_descs.count(); ++i) {
      if (col_descs.at(i).col_id_ == common::OB_APP_MIN_COLUMN_ID + 4) {
        inv_idx_row_desc_.nested_key_col_idx_ = static_cast<int16_t>(i);
        found = true;
        break;
      }
    }
    if (!found) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to find nested_key column in MV layout", K(ret));
    }
  }

  // 7. Init ObSSTableIndexBuilder
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sstable_builder_.init(data_store_desc_))) {
    STORAGE_LOG(WARN, "fail to init sstable index builder", K(ret));
  } else {
    data_store_desc_.sstable_index_builder_ = &sstable_builder_;
  }

  // 8. Init ObMacroSeqParam
  if (OB_FAIL(ret)) {
  } else {
    macro_seq_param_.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param_.start_ = 0;
    macro_seq_param_.interval_ = 1;
    macro_seq_param_.step_ = 1;
  }

  return ret;
}

int ObInvIdxMacroBlockWriterTest::build_reusable_macro_meta(
    ObSSTableIndexBuilder &donor_builder,
    ObInvIdxMacroBlockWriter &donor_writer,
    ObSSTablePrivateObjectCleaner &donor_cleaner,
    common::ObIAllocator &alloc,
    ObDataMacroBlockMeta *&out_meta)
{
  int ret = OB_SUCCESS;
  out_meta = nullptr;
  share::ObPreWarmerParam pre_warm_param(share::PRE_WARM_TYPE_NONE);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 5000;
  seq_param.interval_ = 1;
  seq_param.step_ = 1;
  if (OB_FAIL(donor_builder.init(data_store_desc_))) {
    STORAGE_LOG(WARN, "fail to init donor sstable index builder", K(ret));
  } else if (FALSE_IT(data_store_desc_.sstable_index_builder_ = &donor_builder)) {
  } else if (OB_FAIL(donor_writer.open(inv_idx_row_desc_, data_store_desc_, 0 /*parallel_idx*/,
      seq_param, pre_warm_param, donor_cleaner, nullptr /*callback*/))) {
    STORAGE_LOG(WARN, "fail to open donor writer", K(ret));
  } else {
    ObDatumRow row;
    for (int64_t i = 0; OB_SUCC(ret) && i < 20; ++i) {
      if (OB_FAIL(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, "m",
              1 + static_cast<uint64_t>(i), 1, 100, "p"))) {
        STORAGE_LOG(WARN, "fail to make donor row", K(ret), K(i));
      } else if (OB_FAIL(donor_writer.append_row(row))) {
        STORAGE_LOG(WARN, "fail to append donor row", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(donor_writer.close())) {
      STORAGE_LOG(WARN, "fail to close donor writer", K(ret));
    } else if (OB_SUCC(ret)
        && OB_FAIL(donor_writer.index_block_builder_.macro_meta_.deep_copy(out_meta, alloc))) {
      STORAGE_LOG(WARN, "fail to copy reusable macro meta", K(ret));
    }
  }
  return ret;
}

void ObInvIdxMacroBlockWriterTest::make_reuse_desc(
    ObDataMacroBlockMeta &meta,
    ObMacroBlockDesc &desc)
{
  desc.reset();
  desc.macro_meta_ = &meta;
  desc.macro_block_id_ = meta.get_macro_id();
  desc.is_clustered_index_tree_ = false;
  desc.row_count_ = static_cast<int32_t>(meta.val_.row_count_);
  desc.range_.set_whole_range();
}

// =============================================================================
// Group 1 — Basic lifecycle and argument validation
// =============================================================================

// 1.1 default_constructor_state: Default state of a newly constructed writer.
TEST_F(ObInvIdxMacroBlockWriterTest, default_constructor_state)
{
  ObInvIdxMacroBlockWriter writer;
  EXPECT_FALSE(writer.is_opened());
  EXPECT_EQ(-1, writer.get_last_macro_seq());
  ObMacroBlocksWriteCtx &ctx = writer.get_macro_block_write_ctx();
  EXPECT_TRUE(ctx.is_empty());
}

// 1.2 open_with_invalid_args: open() with an invalid row descriptor.
TEST_F(ObInvIdxMacroBlockWriterTest, open_with_invalid_args)
{
  ObInvIdxMacroBlockWriter writer;
  ObInvIdxRowDesc row_desc;       // not initialized -> invalid
  ObDataStoreDesc data_desc;
  share::ObPreWarmerParam pre_warm_param(share::PRE_WARM_TYPE_NONE);
  ObSSTablePrivateObjectCleaner cleaner;

  int ret = writer.open(
      row_desc,
      data_desc,
      0 /*parallel_idx*/,
      macro_seq_param_,
      pre_warm_param,
      cleaner,
      nullptr /*callback*/);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  EXPECT_FALSE(writer.is_opened());
}

// 1.3 open_with_invalid_data_store_desc: open() with an invalid data store desc.
TEST_F(ObInvIdxMacroBlockWriterTest, open_with_invalid_data_store_desc)
{
  ObInvIdxMacroBlockWriter writer;
  share::ObPreWarmerParam pre_warm_param(share::PRE_WARM_TYPE_NONE);
  ObSSTablePrivateObjectCleaner cleaner;

  prepare_inv_idx_schema(table_schema_);
  ASSERT_EQ(OB_SUCCESS, prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
  ASSERT_TRUE(inv_idx_row_desc_.is_valid());

  macro_seq_param_.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  macro_seq_param_.start_ = 0;
  macro_seq_param_.interval_ = 1;
  macro_seq_param_.step_ = 1;

  ObDataStoreDesc invalid_desc;  // not initialized
  int ret = writer.open(
      inv_idx_row_desc_,
      invalid_desc,
      0 /*parallel_idx*/,
      macro_seq_param_,
      pre_warm_param,
      cleaner,
      nullptr /*callback*/);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
}

// 1.4 operations_on_unopened_writer: Operations on an unopened writer.
TEST_F(ObInvIdxMacroBlockWriterTest, operations_on_unopened_writer)
{
  ObInvIdxMacroBlockWriter writer;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(5));

  EXPECT_EQ(OB_NOT_INIT, writer.append_row(row));
  EXPECT_EQ(OB_NOT_INIT, writer.close());

  ObBatchDatumRows batch;
  EXPECT_EQ(OB_NOT_INIT, writer.append_batch(batch));
}

// 1.5 open_twice: open() called twice.
TEST_F(ObInvIdxMacroBlockWriterTest, open_twice)
{
  ObInvIdxMacroBlockWriter writer;
  share::ObPreWarmerParam pre_warm_param(share::PRE_WARM_TYPE_NONE);
  ObSSTablePrivateObjectCleaner cleaner;

  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_TRUE(data_store_desc_.is_valid());

  int ret = writer.open(
      inv_idx_row_desc_,
      data_store_desc_,
      0 /*parallel_idx*/,
      macro_seq_param_,
      pre_warm_param,
      cleaner,
      nullptr /*callback*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(writer.is_opened());

  ret = writer.open(
      inv_idx_row_desc_,
      data_store_desc_,
      0 /*parallel_idx*/,
      macro_seq_param_,
      pre_warm_param,
      cleaner,
      nullptr /*callback*/);
  EXPECT_EQ(OB_INIT_TWICE, ret);
}

// 1.6 reset_lifecycle: Writer state after close.
TEST_F(ObInvIdxMacroBlockWriterTest, reset_lifecycle)
{
  ObInvIdxMacroBlockWriter writer;
  share::ObPreWarmerParam pre_warm_param(share::PRE_WARM_TYPE_NONE);
  ObSSTablePrivateObjectCleaner cleaner;

  ASSERT_EQ(OB_SUCCESS, prepare_open_env());

  ASSERT_EQ(OB_SUCCESS, writer.open(
      inv_idx_row_desc_,
      data_store_desc_,
      0 /*parallel_idx*/,
      macro_seq_param_,
      pre_warm_param,
      cleaner,
      nullptr /*callback*/));
  ASSERT_TRUE(writer.is_opened());

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_FALSE(writer.is_opened());

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(5));
  EXPECT_EQ(OB_NOT_INIT, writer.append_row(row));
}

// 1.7 successful_append: append() successfully.
TEST_F(ObInvIdxMacroBlockWriterTest, successful_append)
{
  ObInvIdxMacroBlockWriter writer;
  share::ObPreWarmerParam pre_warm_param(share::PRE_WARM_TYPE_NONE);
  ObSSTablePrivateObjectCleaner cleaner;

  ASSERT_EQ(OB_SUCCESS, prepare_open_env());

  ASSERT_EQ(OB_SUCCESS, writer.open(
      inv_idx_row_desc_,
      data_store_desc_,
      0 /*parallel_idx*/,
      macro_seq_param_,
      pre_warm_param,
      cleaner,
      nullptr /*callback*/));
  ASSERT_TRUE(writer.is_opened());
  EXPECT_GE(writer.get_last_macro_seq(), 0);

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "hello", 100, 3, 50, "posdata1"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
  const compaction::ObMergeBlockInfo &block_info = writer.get_merge_block_info();
  EXPECT_GT(block_info.new_micro_count_in_new_macro_, 0);
  EXPECT_GT(block_info.original_size_, 0);
  EXPECT_GT(block_info.compressed_size_, 0);
  EXPECT_GT(block_info.new_micro_info_.get_data_micro_size(), 0);
  EXPECT_GT(block_info.block_io_us_, 0);
  writer.reset();
}

// =============================================================================
// Group 2 — append_batch() entry points
// =============================================================================

// 2.1 append_batch_negative_count: append_batch() with a negative row count.
TEST_F(ObInvIdxMacroBlockWriterTest, append_batch_negative_count)
{
  ObInvIdxMacroBlockWriter writer;
  share::ObPreWarmerParam pre_warm_param(share::PRE_WARM_TYPE_NONE);
  ObSSTablePrivateObjectCleaner cleaner;

  ASSERT_EQ(OB_SUCCESS, prepare_open_env());

  ASSERT_EQ(OB_SUCCESS, writer.open(
      inv_idx_row_desc_,
      data_store_desc_,
      0 /*parallel_idx*/,
      macro_seq_param_,
      pre_warm_param,
      cleaner,
      nullptr /*callback*/));

  ObBatchDatumRows batch;
  batch.row_count_ = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.append_batch(batch));

  ASSERT_EQ(OB_SUCCESS, writer.close());
}

// 2.2 append_empty_batch: append_batch() with an empty batch.
TEST_F(ObInvIdxMacroBlockWriterTest, append_empty_batch)
{
  ObInvIdxMacroBlockWriter writer;
  share::ObPreWarmerParam pre_warm_param(share::PRE_WARM_TYPE_NONE);
  ObSSTablePrivateObjectCleaner cleaner;

  ASSERT_EQ(OB_SUCCESS, prepare_open_env());

  ASSERT_EQ(OB_SUCCESS, writer.open(
      inv_idx_row_desc_,
      data_store_desc_,
      0 /*parallel_idx*/,
      macro_seq_param_,
      pre_warm_param,
      cleaner,
      nullptr /*callback*/));
  ASSERT_TRUE(writer.is_opened());

  ObBatchDatumRows batch;
  batch.row_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, writer.append_batch(batch));

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_FALSE(writer.is_opened());
}

// 2.3 successful_append_batch: append_batch() successfully.
TEST_F(ObInvIdxMacroBlockWriterTest, successful_append_batch)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());

  // Build a vectorized batch via ObRowVectorConverter (VEC_UNIFORM shallow-copies each datum).
  const int64_t batch_size = 6;
  const common::ObIArray<share::schema::ObColDesc> &col_descs =
      data_store_desc_.get_full_stored_col_descs();
  common::ObArray<VectorFormat> vec_formats;
  for (int64_t i = 0; i < col_descs.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_UNIFORM));
  }
  ObRowVectorConverter converter;
  ASSERT_EQ(OB_SUCCESS, converter.init(col_descs, vec_formats, batch_size));

  // One fresh ObDatumRow per batch row — the converter shallow-copies datums, so
  // reusing one row would alias every int-column slot to the last value.
  ObDatumRow rows[6];
  const char *terms[2] = {"a", "b"};
  uint64_t doc_id = 1;
  bool is_full = false;
  int64_t r = 0;
  for (int64_t t = 0; t < 2; ++t) {
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(rows[r], inv_idx_row_desc_, mv_col_cnt_,
          terms[t], doc_id++, 1, 100, "p"));
      ASSERT_EQ(OB_SUCCESS, converter.append_row(rows[r], is_full));
      ++r;
    }
  }

  ObBatchDatumRows batch;
  ASSERT_EQ(OB_SUCCESS, converter.get_batch_datums(batch));
  EXPECT_EQ(batch_size, batch.row_count_);
  ASSERT_EQ(OB_SUCCESS, writer.append_batch(batch));

  EXPECT_TRUE(writer.micro_term_groups_.empty());
  const ObInvIdxLogicalBlockGroup &group = writer.pending_term_group_;
  EXPECT_EQ(1, group.logical_block_metas_.count());
  EXPECT_EQ(3, group.doc_count_);
  EXPECT_EQ(3ul, group.max_doc_id_);
  EXPECT_EQ(3, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_EQ(3, writer.posting_encoder_.get_doc_count());
  EXPECT_TRUE(writer.has_pending_rows_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
}

// =============================================================================
// Group 4 — Logical block and term group layout within a data micro block
// =============================================================================

// 4.1 append_row_same_term_one_logical_block: same term, one logical block.
TEST_F(ObInvIdxMacroBlockWriterTest, append_row_same_term_one_logical_block)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());

  ObDatumRow row;
  for (int64_t i = 1; i <= 200; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "b", 201, 1, 100, "p"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  EXPECT_TRUE(writer.micro_term_groups_.empty());
  const ObInvIdxLogicalBlockGroup &group = writer.pending_term_group_;
  EXPECT_EQ(1, group.logical_block_metas_.count());
  EXPECT_EQ(200, group.doc_count_);
  EXPECT_EQ(200ul, group.max_doc_id_);
  EXPECT_EQ(200, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_EQ(1, writer.posting_encoder_.get_doc_count());
  EXPECT_TRUE(writer.has_pending_rows_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
}

// 4.2 append_row_multiple_terms_one_logical_block: multiple terms, one logical block each.
TEST_F(ObInvIdxMacroBlockWriterTest, append_row_multiple_terms_one_logical_block)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  const char *terms[3] = {"a", "b", "c"};
  uint64_t doc_id = 1;
  for (int64_t t = 0; t < 3; ++t) {
    for (int64_t i = 0; i < 5; ++i) {
      ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
          terms[t], doc_id++, 1, 100, "p"));
      ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
    }
  }
  ASSERT_EQ(1, writer.micro_term_groups_.count());
  EXPECT_EQ(5, writer.micro_term_groups_.at(0).doc_count_);
  EXPECT_EQ(5ul, writer.micro_term_groups_.at(0).max_doc_id_);
  EXPECT_EQ(1, writer.micro_term_groups_.at(0).logical_block_metas_.count());
  EXPECT_EQ(5, writer.pending_term_group_.doc_count_);
  EXPECT_EQ(10ul, writer.pending_term_group_.max_doc_id_);
  EXPECT_EQ(1, writer.pending_term_group_.logical_block_metas_.count());
  EXPECT_EQ(10, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_EQ(5, writer.posting_encoder_.get_doc_count());
  EXPECT_TRUE(writer.has_pending_rows_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
}

// 4.3 append_row_same_term_multiple_logical_blocks: same term, multiple logical blocks.
TEST_F(ObInvIdxMacroBlockWriterTest, append_row_same_term_multiple_logical_blocks)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 300; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "b", 301, 1, 100, "p"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  EXPECT_TRUE(writer.micro_term_groups_.empty());
  const ObInvIdxLogicalBlockGroup &group = writer.pending_term_group_;
  EXPECT_EQ(2, group.logical_block_metas_.count());
  EXPECT_EQ(300, group.doc_count_);
  EXPECT_EQ(300ul, group.max_doc_id_);
  EXPECT_EQ(256, group.logical_block_metas_.at(0).doc_count_);
  EXPECT_EQ(256ul, group.logical_block_metas_.at(0).max_doc_id_);
  EXPECT_EQ(44, group.logical_block_metas_.at(1).doc_count_);
  EXPECT_EQ(257ul, group.logical_block_metas_.at(1).min_doc_id_);
  EXPECT_EQ(300ul, group.logical_block_metas_.at(1).max_doc_id_);
  EXPECT_EQ(300, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_EQ(1, writer.posting_encoder_.get_doc_count());
  EXPECT_TRUE(writer.has_pending_rows_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
}

// 4.4 append_row_multiple_terms_multiple_logical_blocks: multiple terms, multiple logical blocks each.
TEST_F(ObInvIdxMacroBlockWriterTest, append_row_multiple_terms_multiple_logical_blocks)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 300; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  for (int64_t i = 301; i <= 600; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "b", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "c", 601, 1, 100, "p"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  ASSERT_EQ(1, writer.micro_term_groups_.count());
  const ObInvIdxLogicalBlockGroup &group_a = writer.micro_term_groups_.at(0);
  EXPECT_EQ(2, group_a.logical_block_metas_.count());
  EXPECT_EQ(300, group_a.doc_count_);
  EXPECT_EQ(300ul, group_a.max_doc_id_);
  EXPECT_EQ(256, group_a.logical_block_metas_.at(0).doc_count_);
  EXPECT_EQ(1ul, group_a.logical_block_metas_.at(0).min_doc_id_);
  EXPECT_EQ(256ul, group_a.logical_block_metas_.at(0).max_doc_id_);
  EXPECT_EQ(44, group_a.logical_block_metas_.at(1).doc_count_);
  EXPECT_EQ(257ul, group_a.logical_block_metas_.at(1).min_doc_id_);
  EXPECT_EQ(300ul, group_a.logical_block_metas_.at(1).max_doc_id_);
  const ObInvIdxLogicalBlockGroup &group_b = writer.pending_term_group_;
  EXPECT_EQ(2, group_b.logical_block_metas_.count());
  EXPECT_EQ(300, group_b.doc_count_);
  EXPECT_EQ(600ul, group_b.max_doc_id_);
  EXPECT_EQ(256, group_b.logical_block_metas_.at(0).doc_count_);
  EXPECT_EQ(301ul, group_b.logical_block_metas_.at(0).min_doc_id_);
  EXPECT_EQ(556ul, group_b.logical_block_metas_.at(0).max_doc_id_);
  EXPECT_EQ(44, group_b.logical_block_metas_.at(1).doc_count_);
  EXPECT_EQ(557ul, group_b.logical_block_metas_.at(1).min_doc_id_);
  EXPECT_EQ(600ul, group_b.logical_block_metas_.at(1).max_doc_id_);
  EXPECT_EQ(600, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_EQ(1, writer.posting_encoder_.get_doc_count());
  EXPECT_TRUE(writer.macro_stats_.inv_meta_.get_stats().is_empty_range());
  EXPECT_TRUE(writer.has_pending_rows_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
}

// =============================================================================
// Group 4a — Writer state transitions and commit boundaries
// =============================================================================

// 4a.1 append_rows_keeps_logical_block_pending: append_row only changes encoder state.
TEST_F(ObInvIdxMacroBlockWriterTest, append_rows_keeps_logical_block_pending)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }

  EXPECT_TRUE(writer.has_pending_rows_);
  EXPECT_EQ(5, writer.posting_encoder_.get_doc_count());
  EXPECT_EQ(5, writer.posting_value_writer_.get_row_count());
  EXPECT_TRUE(writer.posting_micro_writer_.is_empty());
  EXPECT_TRUE(writer.posting_value_micro_writer_.is_empty());
  EXPECT_TRUE(writer.micro_term_groups_.empty());
  EXPECT_TRUE(writer.pending_term_group_.logical_block_metas_.empty());
  EXPECT_EQ(0, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
}

TEST_F(ObInvIdxMacroBlockWriterTest, invalid_macro_meta_is_rejected_before_writing_index)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "a", 1, 1, 100, "p"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  ASSERT_EQ(OB_SUCCESS, writer.flush_logical_block());
  ASSERT_EQ(OB_SUCCESS, writer.flush_micro_block_pair());
  ASSERT_TRUE(writer.macro_block_.is_dirty());
  ASSERT_TRUE(writer.macro_stats_.is_valid());

  ObInvMacroMeta valid_meta;
  ASSERT_EQ(OB_SUCCESS, valid_meta.assign(writer.macro_stats_.inv_meta_));
  const int64_t data_size = writer.macro_block_.get_data_size();
  const int64_t index_offset = writer.index_block_builder_.index_block_offset_;
  const int64_t row_offset = writer.index_block_builder_.row_offset_;
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, writer.macro_stats_.inv_meta_.assign(valid_meta));
    if (0 == i) {
      writer.macro_stats_.inv_meta_.reset();
    } else {
      writer.macro_stats_.inv_meta_.reuse();
      ASSERT_TRUE(writer.macro_stats_.inv_meta_.get_stats().is_valid());
      ASSERT_FALSE(writer.macro_stats_.inv_meta_.is_valid());
    }
    ASSERT_TRUE(writer.macro_stats_.endkey_.is_valid());
    ASSERT_GT(writer.macro_stats_.doc_count_, 0);
    int64_t estimate_size = -1;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
        writer.index_block_builder_.estimate_projected_macro_meta_block_size(
            writer.macro_stats_, estimate_size));
    EXPECT_EQ(0, estimate_size);
    EXPECT_EQ(OB_INVALID_ARGUMENT, writer.flush_macro_block(true /*is_close_flush*/));
    EXPECT_EQ(data_size, writer.macro_block_.get_data_size());
    EXPECT_EQ(index_offset, writer.index_block_builder_.index_block_offset_);
    EXPECT_EQ(row_offset, writer.index_block_builder_.row_offset_);
    EXPECT_EQ(0, writer.get_macro_block_write_ctx().get_macro_block_count());
  }

  // Rejected inputs must leave the buffered macro usable with its original metadata.
  ASSERT_EQ(OB_SUCCESS, writer.macro_stats_.inv_meta_.assign(valid_meta));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
}

// 4a.2 flush_logical_block_updates_candidate_only: logical-block statistics stay uncommitted.
TEST_F(ObInvIdxMacroBlockWriterTest, flush_logical_block_updates_candidate_only)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.flush_logical_block());

  EXPECT_FALSE(writer.has_pending_rows_);
  EXPECT_EQ(0, writer.posting_encoder_.get_doc_count());
  EXPECT_EQ(0, writer.posting_value_writer_.get_row_count());
  EXPECT_EQ(1, writer.posting_micro_writer_.get_block_count());
  EXPECT_EQ(1, writer.posting_value_micro_writer_.get_block_count());
  EXPECT_TRUE(writer.micro_term_groups_.empty());
  ASSERT_EQ(1, writer.pending_term_group_.logical_block_metas_.count());
  EXPECT_EQ(5, writer.pending_term_group_.doc_count_);
  EXPECT_EQ(5ul, writer.pending_term_group_.max_doc_id_);
  EXPECT_EQ(5, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(1ul, writer.current_micro_stats_.inv_meta_.get_stats().min_doc_id_);
  EXPECT_EQ(5ul, writer.current_micro_stats_.inv_meta_.get_stats().max_doc_id_);
  EXPECT_EQ(5ul, writer.current_micro_stats_.inv_meta_.get_stats().total_doc_length_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_EQ(0, writer.micro_index_size_);
  EXPECT_GT(writer.pending_index_size_, 0);
  EXPECT_TRUE(writer.macro_stats_.inv_meta_.get_stats().is_empty_range());
  EXPECT_TRUE(writer.seg_inv_meta_.get_stats().is_empty_range());

  ASSERT_EQ(OB_SUCCESS, writer.close());
}

// 4a.3 flush_micro_block_pair_commits_candidate: posting, value and index commit together.
TEST_F(ObInvIdxMacroBlockWriterTest, flush_micro_block_pair_commits_candidate)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.flush_logical_block());
  ASSERT_NE(nullptr, writer.index_block_builder_.actual_index_micro_writer_);
  ASSERT_NE(nullptr, writer.index_block_builder_.trial_index_micro_writer_);
  EXPECT_EQ(0, writer.index_block_builder_.actual_index_micro_writer_->get_row_count());
  EXPECT_EQ(0, writer.index_block_builder_.trial_index_micro_writer_->get_row_count());

  ASSERT_EQ(OB_SUCCESS, writer.flush_micro_block_pair());

  EXPECT_TRUE(writer.posting_micro_writer_.is_empty());
  EXPECT_TRUE(writer.posting_value_micro_writer_.is_empty());
  EXPECT_TRUE(writer.micro_term_groups_.empty());
  EXPECT_TRUE(writer.pending_term_group_.logical_block_metas_.empty());
  EXPECT_EQ(0, writer.current_micro_stats_.doc_count_);
  EXPECT_TRUE(writer.current_micro_stats_.inv_meta_.get_stats().is_empty_range());
  EXPECT_EQ(0, writer.micro_index_size_);
  EXPECT_EQ(0, writer.pending_index_size_);
  EXPECT_EQ(5, writer.macro_stats_.doc_count_);
  EXPECT_EQ(1ul, writer.macro_stats_.inv_meta_.get_stats().min_doc_id_);
  EXPECT_EQ(5ul, writer.macro_stats_.inv_meta_.get_stats().max_doc_id_);
  EXPECT_EQ(5ul, writer.macro_stats_.inv_meta_.get_stats().total_doc_length_);
  EXPECT_TRUE(writer.seg_inv_meta_.get_stats().is_empty_range());
  EXPECT_EQ(2, writer.macro_block_.get_micro_block_count());
  EXPECT_EQ(1, writer.index_block_builder_.actual_index_micro_writer_->get_row_count());
  EXPECT_EQ(1, writer.index_block_builder_.trial_index_micro_writer_->get_row_count());
  EXPECT_EQ(writer.index_block_builder_.actual_index_micro_writer_->get_block_size(),
      writer.index_block_builder_.trial_index_micro_writer_->get_block_size());
  EXPECT_EQ(4, writer.index_block_builder_.row_offset_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_EQ(1ul, writer.get_seg_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(5ul, writer.get_seg_inv_meta().get_stats().max_doc_id_);
  EXPECT_EQ(5ul, writer.get_seg_inv_meta().get_stats().total_doc_length_);
}

// 4a.4 close_flushes_all_pending_levels_once: close drains encoder, pair and macro exactly once.
TEST_F(ObInvIdxMacroBlockWriterTest, close_flushes_all_pending_levels_once)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 7; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_TRUE(writer.has_pending_rows_);
  ASSERT_EQ(OB_SUCCESS, writer.close());

  EXPECT_FALSE(writer.is_opened());
  EXPECT_EQ(7, writer.get_merge_block_info().total_row_count_);
  EXPECT_EQ(1, writer.get_merge_block_info().macro_block_count_);
  EXPECT_EQ(1ul, writer.get_seg_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(7ul, writer.get_seg_inv_meta().get_stats().max_doc_id_);
  EXPECT_EQ(7ul, writer.get_seg_inv_meta().get_stats().total_doc_length_);
  EXPECT_EQ(OB_NOT_INIT, writer.close());
  EXPECT_EQ(7, writer.get_merge_block_info().total_row_count_);
  EXPECT_EQ(1, writer.get_merge_block_info().macro_block_count_);
}

// =============================================================================
// Group 4b — BM25 write-path integration
// =============================================================================

// 4b.1 bm25_frontier_persisted_in_index_agg_data: the encoded frontier reaches the index row.
TEST_F(ObInvIdxMacroBlockWriterTest, bm25_frontier_persisted_in_index_agg_data)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), static_cast<uint64_t>(10 * i),
        static_cast<uint64_t>(100 * i), "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());

  ObIndexBlockRowParser parser;
  const ObIndexBlockRowHeader *header = nullptr;
  const char *agg_buf = nullptr;
  int64_t agg_size = 0;
  ASSERT_EQ(OB_SUCCESS, parser.init(
      writer.index_block_builder_.row_builder_.rowkey_column_count_,
      writer.index_block_builder_.row_builder_.row_));
  ASSERT_EQ(OB_SUCCESS, parser.get_header(header));
  ASSERT_NE(nullptr, header);
  EXPECT_TRUE(header->is_inverted_index());
  ASSERT_EQ(OB_SUCCESS, parser.get_agg_row(agg_buf, agg_size));

  common::ObSEArray<ObInvIdxLogicalBlockMeta, 4> metas;
  int64_t posting_value_offset = 0;
  int64_t posting_value_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataDecoder::decode(
      agg_buf, agg_size, metas, posting_value_offset, posting_value_size));
  EXPECT_GT(posting_value_offset, 0);
  EXPECT_GT(posting_value_size, 0);
  ASSERT_EQ(1, metas.count());
  const ObInvIdxLogicalBlockMeta &meta = metas.at(0);
  EXPECT_EQ(5, meta.doc_count_);
  EXPECT_EQ(1ul, meta.min_doc_id_);
  EXPECT_EQ(5ul, meta.max_doc_id_);
  EXPECT_EQ(150ul, meta.total_doc_length_);
  ASSERT_EQ(5, meta.frontier_pairs_.count());
  for (int64_t i = 0; i < meta.frontier_pairs_.count(); ++i) {
    EXPECT_EQ(static_cast<uint32_t>(10 * (i + 1)), meta.frontier_pairs_.at(i).token_freq_);
    EXPECT_EQ(static_cast<uint32_t>(100 * (i + 1)), meta.frontier_pairs_.at(i).doc_length_);
  }
}

// =============================================================================
// Group 5 — Data micro block boundary
// =============================================================================

// 5.1 append_row_same_term_across_micro_blocks: same term spanning multiple data micro blocks.
TEST_F(ObInvIdxMacroBlockWriterTest, append_row_same_term_across_micro_blocks)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  const int64_t row_cnt = 8000;
  const uint64_t big_val = 1000000;  // forces 3-byte packing in the posting block
  for (int64_t i = 1; i <= row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), big_val, big_val, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }

  // At least one posting/value pair is committed while more rows of the same term stay pending.
  EXPECT_GE(writer.macro_block_.get_micro_block_count(), 2);
  EXPECT_GT(writer.macro_stats_.doc_count_, 0);
  EXPECT_TRUE(writer.pending_term_group_.is_valid());
  EXPECT_TRUE(writer.current_term_.is_valid());
  EXPECT_TRUE(writer.has_pending_rows_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
  EXPECT_EQ(row_cnt, writer.get_merge_block_info().total_row_count_);
  EXPECT_EQ(1ul, writer.get_seg_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(static_cast<uint64_t>(row_cnt), writer.get_seg_inv_meta().get_stats().max_doc_id_);
  EXPECT_EQ(static_cast<uint64_t>(row_cnt) * big_val,
      writer.get_seg_inv_meta().get_stats().total_doc_length_);
}

// 5.2 full_macro_retries_candidate_on_next_macro: a rejected pair is retried without loss.
TEST_F(ObInvIdxMacroBlockWriterTest, full_macro_retries_candidate_on_next_macro)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  const int64_t row_cnt = 600;
  const int64_t position_list_size = data_store_desc_.get_macro_block_size()
      / (2 * DEFAULT_POSTING_BLOCK_CAPACITY);
  char *position_list = static_cast<char *>(allocator_.alloc(position_list_size));
  ASSERT_NE(nullptr, position_list);
  MEMSET(position_list, 'p', position_list_size);
  position_list[position_list_size - 1] = '\0';

  ObDatumRow row;
  for (int64_t i = 1; i <= row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, position_list));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());

  EXPECT_GE(writer.get_merge_block_info().macro_block_count_, 2);
  EXPECT_EQ(row_cnt, writer.get_merge_block_info().total_row_count_);
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_GE(writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_
      ->get_macro_block_count(), 2);
  EXPECT_EQ(1ul, writer.get_seg_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(static_cast<uint64_t>(row_cnt), writer.get_seg_inv_meta().get_stats().max_doc_id_);
  EXPECT_EQ(static_cast<uint64_t>(row_cnt),
      writer.get_seg_inv_meta().get_stats().total_doc_length_);
}

// 5.3 oversized_pair_fails_on_empty_macro: an empty macro is not switched repeatedly.
TEST_F(ObInvIdxMacroBlockWriterTest, oversized_pair_fails_on_empty_macro)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(
      row, inv_idx_row_desc_, mv_col_cnt_, "a", 1, 1, 100, "p"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  ASSERT_EQ(OB_SUCCESS, writer.flush_logical_block());
  ASSERT_EQ(1, writer.current_micro_stats_.doc_count_);

  // Make the empty macro expose less room than the already-built candidate pair.
  writer.macro_block_.data_base_offset_ = data_store_desc_.get_macro_block_size() - 1;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, writer.flush_micro_block_pair());

  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_EQ(1, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_block_.get_micro_block_count());
  EXPECT_EQ(0, writer.get_merge_block_info().macro_block_count_);
  EXPECT_EQ(0, writer.index_block_builder_.actual_index_micro_writer_->get_row_count());
  EXPECT_EQ(0, writer.index_block_builder_.trial_index_micro_writer_->get_row_count());
  EXPECT_FALSE(writer.posting_micro_writer_.is_empty());
  EXPECT_FALSE(writer.posting_value_micro_writer_.is_empty());
  ASSERT_EQ(1, writer.micro_term_groups_.count());
  EXPECT_TRUE(writer.pending_term_group_.logical_block_metas_.empty());

  writer.reset();
  EXPECT_FALSE(writer.is_opened());
  EXPECT_TRUE(writer.get_macro_block_write_ctx().is_empty());
}

// =============================================================================
// Group 6 — Schema column variants
// =============================================================================

// 6.1 append_row_without_pos_list: schema without pos_list (no posting value).
TEST_F(ObInvIdxMacroBlockWriterTest, append_row_without_pos_list)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(false /*has_pos_list*/));
  ASSERT_FALSE(inv_idx_row_desc_.has_posting_value());
  ASSERT_LT(inv_idx_row_desc_.pos_list_col_idx_, 0);
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());

  ObDatumRow row;
  for (int64_t i = 1; i <= 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "" /*pos_list unused*/));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "b", 11, 1, 100, ""));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  EXPECT_TRUE(writer.micro_term_groups_.empty());
  const ObInvIdxLogicalBlockGroup &group = writer.pending_term_group_;
  EXPECT_EQ(1, group.logical_block_metas_.count());
  EXPECT_EQ(10, group.doc_count_);
  EXPECT_EQ(10ul, group.max_doc_id_);
  EXPECT_EQ(10, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_TRUE(writer.posting_value_micro_writer_.is_empty());
  EXPECT_EQ(1, writer.posting_encoder_.get_doc_count());
  EXPECT_TRUE(writer.has_pending_rows_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
}

// 6.2 append_row_doc_id_only: schema with only term and doc_id (no BM25).
TEST_F(ObInvIdxMacroBlockWriterTest, append_row_doc_id_only)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(false /*has_pos_list*/, false /*has_relevance*/));
  ASSERT_FALSE(inv_idx_row_desc_.has_posting_value());
  ASSERT_LT(inv_idx_row_desc_.token_freq_col_idx_, 0);
  ASSERT_LT(inv_idx_row_desc_.doc_length_col_idx_, 0);
  ASSERT_LT(inv_idx_row_desc_.pos_list_col_idx_, 0);
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());

  ObDatumRow row;
  for (int64_t i = 1; i <= 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 0 /*tf unused*/, 0 /*dl unused*/, "" /*pos_list unused*/));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "b", 11, 0, 0, ""));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  EXPECT_TRUE(writer.micro_term_groups_.empty());
  const ObInvIdxLogicalBlockGroup &group = writer.pending_term_group_;
  EXPECT_EQ(1, group.logical_block_metas_.count());
  EXPECT_EQ(10, group.doc_count_);
  EXPECT_EQ(10ul, group.max_doc_id_);
  EXPECT_EQ(0, group.logical_block_metas_.at(0).frontier_pairs_.count());
  EXPECT_EQ(0ul, group.logical_block_metas_.at(0).total_doc_length_);
  EXPECT_EQ(10, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_TRUE(writer.posting_value_micro_writer_.is_empty());
  EXPECT_EQ(1, writer.posting_encoder_.get_doc_count());
  EXPECT_TRUE(writer.has_pending_rows_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
}

// =============================================================================
// Group 7 — Compression and round-trip verification
// =============================================================================
// 7.1 compress_lz4_round_trip: real LZ4 compressor + ENCODING_AND_COMPRESSION verify level.
// Each data micro block is compressed and then decompressed-and-compared on the write path
// (check_micro_block), giving a compression round-trip correctness signal without reading the
// sstable back. The micro-block flush boundary is decided by the uncompressed payload estimate,
// so 8000 rows still yield >= 2 data micro blocks under compression.
TEST_F(ObInvIdxMacroBlockWriterTest, compress_lz4_round_trip)
{
  VerifyLevelGuard lvl_guard(MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION);
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(true /*has_pos_list*/, true /*has_relevance*/, "lz4"));
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());

  ASSERT_EQ(OB_SUCCESS, append_compress_round_trip_rows(writer, inv_idx_row_desc_, mv_col_cnt_));
  // At least one data micro block was compressed + round-trip verified mid-stream.
  EXPECT_GE(writer.macro_block_.get_micro_block_count(), 2);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_GE(writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count(), 1);
}

// 7.2 compress_zstd_round_trip: same as 7.1 with the ZSTD compressor.
TEST_F(ObInvIdxMacroBlockWriterTest, compress_zstd_round_trip)
{
  VerifyLevelGuard lvl_guard(MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION);
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(true /*has_pos_list*/, true /*has_relevance*/, "zstd"));
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());

  ASSERT_EQ(OB_SUCCESS, append_compress_round_trip_rows(writer, inv_idx_row_desc_, mv_col_cnt_));
  EXPECT_GE(writer.macro_block_.get_micro_block_count(), 2);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_GE(writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count(), 1);
}

// =============================================================================
// Group 8 — Macro block id pre-allocation (SS adaptation)
// =============================================================================

// 8.1 pre_alloc_before_flush: the macro id is allocated before the macro is flushed.
//
// Inverted index data macro blocks are is_for_sstable_data(), so is_pre_alloc() is true and
// the macro id is lazily allocated by alloc_block() at the first data micro block write. After
// enough rows to flush at least one data micro block (but still within one macro), the id must
// already be valid BEFORE close() flushes the macro to disk. Before the adaptation the id was
// invalid here (allocated only at flush time).
TEST_F(ObInvIdxMacroBlockWriterTest, pre_alloc_before_flush)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());
  EXPECT_TRUE(writer.is_pre_alloc());

  ObDatumRow row;
  const int64_t row_cnt = 8000;
  const uint64_t big_val = 1000000;  // 3-byte packing in the posting block
  for (int64_t i = 1; i <= row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), big_val, big_val, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }

  // At least one data micro block has been flushed into the still-open macro, so the macro
  // id must have been pre-allocated before the macro is flushed to disk.
  EXPECT_GE(writer.macro_block_.get_micro_block_count(), 1);
  EXPECT_TRUE(writer.macro_handle_.get_macro_id().is_valid());

  ASSERT_EQ(OB_SUCCESS, writer.close());
  ObMacroBlocksWriteCtx *ctx = writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_;
  EXPECT_NE(nullptr, ctx);
  EXPECT_EQ(1, ctx->get_macro_block_count());
  common::ObSEArray<MacroBlockId, 4> macro_ids;
  EXPECT_EQ(OB_SUCCESS, ctx->get_macro_id_array(macro_ids));
  ASSERT_EQ(1, macro_ids.count());
  EXPECT_TRUE(macro_ids.at(0).is_valid());
  writer.reset();
}

// 8.2 no_pre_alloc: an empty macro never allocates an id (lazy pre-alloc, no leak).
//
// open() then close() with no rows appended: the macro is never dirtied, so alloc_block() is
// never reached and no macro id is allocated. This guards against eager pre-allocation leaking
// an id for a macro that is never written.
TEST_F(ObInvIdxMacroBlockWriterTest, no_pre_alloc)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());
  EXPECT_FALSE(writer.macro_handle_.get_macro_id().is_valid());

  ASSERT_EQ(OB_SUCCESS, writer.close());
  ObMacroBlocksWriteCtx &ctx = writer.get_macro_block_write_ctx();
  EXPECT_TRUE(ctx.is_empty());
  EXPECT_EQ(0, ctx.get_macro_block_count());
}

// 8.3 pre_alloc_one_macro: a minimal single-macro write produces one valid macro id.
//
// Baseline for the block_write_ctx_ id path with a tiny dataset (one data micro block, one
// macro), complementary to 8.1 which stresses the mid-stream timing with many micro blocks.
TEST_F(ObInvIdxMacroBlockWriterTest, pre_alloc_one_macro)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "b", 11, 1, 100, "p"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  ASSERT_EQ(OB_SUCCESS, writer.close());
  ObMacroBlocksWriteCtx *ctx = writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_;
  EXPECT_NE(nullptr, ctx);
  EXPECT_EQ(1, ctx->get_macro_block_count());
  common::ObSEArray<MacroBlockId, 4> macro_ids;
  EXPECT_EQ(OB_SUCCESS, ctx->get_macro_id_array(macro_ids));
  ASSERT_EQ(1, macro_ids.count());
  EXPECT_TRUE(macro_ids.at(0).is_valid());
  writer.reset();
}

// 8.4 pre_alloc_multiple_macros: crossing the macro boundary exercises the reuse branch.
//
// The macro block size is shrunk so a few tens of thousands of rows cross the macro boundary
// and force mid-stream macro flushes, exercising flush_macro_block's reuse branch (which resets
// macro_handle_ for the next macro; the next macro's id is re-allocated lazily). 128KB still
// fits one posting/posting-value data micro block pair plus the in-macro index micro block and
// macro meta. Both the size and the row count are tunable if the >=2 macros assertion misses.
TEST_F(ObInvIdxMacroBlockWriterTest, pre_alloc_multiple_macros)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  // Shrink the macro block size to force multiple macros with a tractable row count.
  static_desc_.macro_block_size_ = 128 * 1024;
  static_desc_.macro_store_size_ = 128 * 1024;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_pre_alloc());

  ObDatumRow row;
  const int64_t row_cnt = 50000;
  const uint64_t big_val = 1000000;
  for (int64_t i = 1; i <= row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), big_val, big_val, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  // Note: the current (last) macro's macro_handle_ may be invalid here — lazy pre-allocation
  // only allocates once a data micro block is flushed into the macro, and the last macro may
  // still hold only pending encoder rows. Its id is allocated during close(). Hence the
  // pre-allocation invariant is asserted post-close via block_write_ctx_ below.

  ASSERT_EQ(OB_SUCCESS, writer.close());
  ObMacroBlocksWriteCtx *ctx = writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_;
  EXPECT_NE(nullptr, ctx);
  EXPECT_GE(ctx->get_macro_block_count(), 2);
  common::ObSEArray<MacroBlockId, 8> macro_ids;
  EXPECT_EQ(OB_SUCCESS, ctx->get_macro_id_array(macro_ids));
  EXPECT_EQ(ctx->get_macro_block_count(), macro_ids.count());
  // Every id is valid and the ids are mutually distinct.
  for (int64_t i = 0; i < macro_ids.count(); ++i) {
    EXPECT_TRUE(macro_ids.at(i).is_valid());
    for (int64_t j = i + 1; j < macro_ids.count(); ++j) {
      EXPECT_FALSE(macro_ids.at(i) == macro_ids.at(j));
    }
  }
  writer.reset();
}

// =============================================================================
// Group 9 — logic_micro_id fill for the n-1 data index row (SS adaptation)
// =============================================================================
//
// The fill is gated in prepare_micro_block_pair by is_major_merge_type().
// Both cases drive one term ("a") so exactly one (M, T) index row is produced; the row builder
// retains the last built serialized index row in row_builder_.row_ (close() does not reuse it),
// so we parse that row's header in place — no SSTable reader (not yet implemented) is needed.

// 9.1 logic_micro_id_not_filled_non_major: default fixture (MINI_MERGE) does not satisfy the gate,
// so the n-1 index row carries no valid logic micro id.
TEST_F(ObInvIdxMacroBlockWriterTest, logic_micro_id_not_filled_non_major)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());  // default: MINI_MERGE, cluster_version 1
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());

  ObIndexBlockRowParser parser;
  const ObIndexBlockRowHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, parser.init(
      writer.index_block_builder_.row_builder_.rowkey_column_count_,
      writer.index_block_builder_.row_builder_.row_));
  ASSERT_EQ(OB_SUCCESS, parser.get_header(header));
  ASSERT_NE(nullptr, header);
  EXPECT_TRUE(header->is_data_block());
  EXPECT_FALSE(header->has_valid_logic_micro_id());
  writer.reset();
}

// 9.2 logic_micro_id_filled_major: major fixture (MAJOR_MERGE) passes the gate, so the
// n-1 index row carries a valid logic micro id whose offset equals the row's block_offset_
// (both are the posting micro block offset), whose logic macro id matches the tablet/version,
// and whose data checksum is the (non-zero) posting block checksum.
TEST_F(ObInvIdxMacroBlockWriterTest, logic_micro_id_filled_major)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(true /*has_pos_list*/, true /*has_relevance*/,
      "none", compaction::MAJOR_MERGE));
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "a", static_cast<uint64_t>(i), 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());

  ObIndexBlockRowParser parser;
  const ObIndexBlockRowHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, parser.init(
      writer.index_block_builder_.row_builder_.rowkey_column_count_,
      writer.index_block_builder_.row_builder_.row_));
  ASSERT_EQ(OB_SUCCESS, parser.get_header(header));
  ASSERT_NE(nullptr, header);
  EXPECT_TRUE(header->is_data_block());
  ASSERT_TRUE(header->has_valid_logic_micro_id());
  const ObLogicMicroBlockId &logic_micro_id = header->get_logic_micro_id();
  // logic micro id offset and the index row block_offset_ are both the posting micro block offset.
  EXPECT_EQ(header->get_block_offset(), logic_micro_id.offset_);
  EXPECT_EQ(mock_tablet_id_.id(), logic_micro_id.logic_macro_id_.tablet_id_);
  EXPECT_EQ(data_store_desc_.get_logical_version(), logic_micro_id.logic_macro_id_.logic_version_);
  EXPECT_NE(0, header->get_data_checksum());
  writer.reset();
}

TEST_F(ObInvIdxMacroBlockWriterTest, logical_generation_matches_header_meta_and_micro_index)
{
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(true, true, "none", compaction::MAJOR_MERGE));
  static_desc_.set_sstable_logic_seq(2);
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  for (int64_t physical_seq = 0; physical_seq < 2; ++physical_seq) {
    ObMacroDataSeq expected(physical_seq);
    ASSERT_EQ(OB_SUCCESS, expected.set_sstable_seq(2));
    EXPECT_EQ(physical_seq, writer.get_last_macro_seq());
    EXPECT_EQ(expected.macro_data_seq_, writer.macro_block_.get_current_macro_seq());
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
                                           "a", physical_seq + 1, 1, 100, "p"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
    ASSERT_EQ(OB_SUCCESS, writer.flush_logical_block());
    ASSERT_EQ(OB_SUCCESS, writer.flush_micro_block_pair());

    ObIndexBlockRowParser parser;
    const ObIndexBlockRowHeader *header = nullptr;
    ASSERT_EQ(OB_SUCCESS, parser.init(writer.index_block_builder_.row_builder_.rowkey_column_count_,
                                      writer.index_block_builder_.row_builder_.row_));
    ASSERT_EQ(OB_SUCCESS, parser.get_header(header));
    ASSERT_TRUE(header->has_valid_logic_micro_id());
    EXPECT_EQ(expected.macro_data_seq_, header->get_logic_micro_id().logic_macro_id_.data_seq_.macro_data_seq_);
    ASSERT_EQ(OB_SUCCESS, writer.flush_macro_block(true /*is_close_flush*/));
    EXPECT_EQ(expected.macro_data_seq_, writer.index_block_builder_.macro_meta_.val_.logic_id_.data_seq_.macro_data_seq_);
    ASSERT_EQ(OB_SUCCESS, writer.switch_to_next_macro_block());
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
}

TEST_F(ObInvIdxMacroBlockWriterTest, logical_generation_preserves_physical_sequence_bits)
{
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(true, true, "none", compaction::INC_MAJOR_MERGE));
  static_desc_.exec_mode_ = compaction::EXEC_MODE_OUTPUT;
  static_desc_.set_sstable_logic_seq(2);
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  const int64_t physical_seq = ObMacroDataSeq::VERSION_LOCAL_SEQ_LIMIT - 1;
  int64_t logical_seq = -1;
  ASSERT_EQ(OB_SUCCESS, writer.build_logic_macro_seq(physical_seq, logical_seq));
  ObMacroDataSeq encoded(logical_seq);
  EXPECT_EQ(physical_seq, encoded.get_version_local_seq());
  EXPECT_EQ(2, static_cast<int64_t>(encoded.sstable_logic_seq_));
  EXPECT_TRUE(encoded.is_data_block());
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.build_logic_macro_seq(-1, logical_seq));
  EXPECT_EQ(OB_SIZE_OVERFLOW, writer.build_logic_macro_seq(ObMacroDataSeq::VERSION_LOCAL_SEQ_LIMIT, logical_seq));
  static_desc_.set_sstable_logic_seq(-1);
  ASSERT_EQ(OB_SUCCESS, writer.build_logic_macro_seq(physical_seq, logical_seq));
  EXPECT_EQ(physical_seq, logical_seq);
  ASSERT_EQ(OB_SUCCESS, writer.close());
}

// =============================================================================
// Group 10 — Search index write path
// =============================================================================
// Search index: term = (COL_IDX, PATH, VALUE) [3-col], posting = DOC_ID,
// posting value = nested_key_list (opaque blob like pos_list), no BM25 params.
// index_type_ = INV_IDX_TYPE_SEARCH. These cases verify the type-system change
// (is_fts_index_ -> index_type_) and the nested_key write path end to end.

// 10.1 search_index_inv_meta_type: init_inv_meta propagates INV_IDX_TYPE_SEARCH
// and the has_relevance_params/has_nested_key flags from the row desc.
TEST_F(ObInvIdxMacroBlockWriterTest, search_index_inv_meta_type)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_search_open_env());
  ASSERT_EQ(INV_IDX_TYPE_SEARCH, inv_idx_row_desc_.index_type_);
  ASSERT_FALSE(inv_idx_row_desc_.has_posting_value() && inv_idx_row_desc_.pos_list_col_idx_ >= 0);
  ASSERT_GE(inv_idx_row_desc_.nested_key_col_idx_, 0);
  ASSERT_LT(inv_idx_row_desc_.token_freq_col_idx_, 0);
  ASSERT_LT(inv_idx_row_desc_.doc_length_col_idx_, 0);
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());

  EXPECT_EQ(INV_IDX_TYPE_SEARCH, writer.current_micro_stats_.inv_meta_.get_stats().index_type_);
  EXPECT_EQ(INV_IDX_TYPE_SEARCH, writer.macro_stats_.inv_meta_.get_stats().index_type_);
  EXPECT_EQ(INV_IDX_TYPE_SEARCH, writer.seg_inv_meta_.get_stats().index_type_);
  EXPECT_FALSE(writer.macro_stats_.inv_meta_.get_stats().has_relevance_params_);
  EXPECT_FALSE(writer.macro_stats_.inv_meta_.get_stats().has_position_list_);
  EXPECT_TRUE(writer.macro_stats_.inv_meta_.get_stats().has_nested_key_);
  EXPECT_TRUE(writer.current_micro_stats_.inv_meta_.get_stats().has_nested_key_);
  EXPECT_TRUE(writer.macro_stats_.inv_meta_.get_stats().is_empty_range());
  EXPECT_TRUE(writer.seg_inv_meta_.get_stats().is_empty_range());

  ASSERT_EQ(OB_SUCCESS, writer.close());
  writer.reset();
}

// 10.1.1 search_text_open_propagates_capabilities: open accepts the three-column
// posting layout of a search text row desc and propagates its static capabilities.
// The FTS fixture only provides the TF/DL/POS storage layout; no rows are appended.
TEST_F(ObInvIdxMacroBlockWriterTest, search_text_open_propagates_capabilities)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(true /*has_pos_list*/, true /*has_relevance*/));
  inv_idx_row_desc_.index_type_ = INV_IDX_TYPE_SEARCH_TEXT;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());

  EXPECT_EQ(INV_IDX_TYPE_SEARCH_TEXT, writer.current_micro_stats_.inv_meta_.get_stats().index_type_);
  EXPECT_EQ(INV_IDX_TYPE_SEARCH_TEXT, writer.macro_stats_.inv_meta_.get_stats().index_type_);
  EXPECT_EQ(INV_IDX_TYPE_SEARCH_TEXT, writer.seg_inv_meta_.get_stats().index_type_);
  EXPECT_TRUE(writer.seg_inv_meta_.get_stats().has_relevance_params_);
  EXPECT_TRUE(writer.seg_inv_meta_.get_stats().has_position_list_);
  EXPECT_FALSE(writer.seg_inv_meta_.get_stats().has_nested_key_);
  EXPECT_TRUE(writer.seg_inv_meta_.is_valid());
  EXPECT_TRUE(writer.seg_inv_meta_.get_stats().is_empty_range());

  ASSERT_EQ(OB_SUCCESS, writer.close());
  writer.reset();
}

// 10.1.2 search_text_open_rejects_doc_id_only_layout: the current search text writer
// only produces DOC_ID/TF/DL postings, so a DOC_ID-only layout is rejected at open.
TEST_F(ObInvIdxMacroBlockWriterTest, search_text_open_rejects_doc_id_only_layout)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(false /*has_pos_list*/, false /*has_relevance*/));
  inv_idx_row_desc_.index_type_ = INV_IDX_TYPE_SEARCH_TEXT;
  ASSERT_EQ(OB_INVALID_ARGUMENT, open_writer(writer, cleaner));
  ASSERT_FALSE(writer.is_opened());
}

struct SearchTextTestRow
{
  int64_t col_idx_;
  const char *path_;
  const char *token_;
  uint64_t doc_id_;
  uint64_t token_freq_;
  uint64_t doc_length_;
  const char *pos_list_;
};

// Locate the posting micro block and, when present, the posting value micro block that
// immediately follows it in the single in-memory data macro block.
static void locate_search_text_micro_blocks(
    ObInvIdxMacroBlockWriter &writer,
    const bool has_pos_list,
    const char *&posting_micro_block,
    int64_t &posting_micro_block_size,
    const char *&posting_value_micro_block,
    int64_t &posting_value_micro_block_size)
{
  posting_value_micro_block = nullptr;
  posting_value_micro_block_size = 0;
  ASSERT_EQ(has_pos_list ? 2 : 1, writer.macro_block_.get_micro_block_count());
  posting_micro_block = writer.macro_block_.get_micro_block_data_ptr();
  const int64_t micro_block_data_size = writer.macro_block_.get_micro_block_data_size();
  ObMicroBlockHeader posting_header;
  int64_t header_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      posting_header.deserialize(posting_micro_block, micro_block_data_size, header_pos));
  ASSERT_EQ(MICRO_BLOCK_INV_IDX_POSTING,
      static_cast<ObMicroBlockInvertedIndexType>(posting_header.inverted_index_type_));
  posting_micro_block_size = posting_header.header_size_ + posting_header.data_zlength_;
  // The macro data region also carries the index/meta blocks appended by
  // write_index_and_meta_blocks() after the data micro blocks, so the posting
  // (and posting value) blocks only cover the data prefix, not the whole region.
  if (!has_pos_list) {
    ASSERT_LT(posting_micro_block_size, micro_block_data_size);
  } else {
    ASSERT_LT(posting_micro_block_size, micro_block_data_size);
    posting_value_micro_block = posting_micro_block + posting_micro_block_size;
    const int64_t remain_size = micro_block_data_size - posting_micro_block_size;
    ObMicroBlockHeader posting_value_header;
    header_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        posting_value_header.deserialize(posting_value_micro_block, remain_size, header_pos));
    ASSERT_EQ(MICRO_BLOCK_INV_IDX_POSTING_VALUE,
        static_cast<ObMicroBlockInvertedIndexType>(posting_value_header.inverted_index_type_));
    posting_value_micro_block_size =
        posting_value_header.header_size_ + posting_value_header.data_zlength_;
    ASSERT_LT(posting_value_micro_block_size, remain_size);
  }
}

TEST_F(ObInvIdxMacroBlockWriterTest, search_text_rejects_invalid_datum_before_mutation)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_search_text_open_env(true /*has_pos_list*/));
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ObDatumRow row;
  // Reject NULL/NOP only in encoded columns; multi-version columns are not encoded.
  for (int64_t i = 0; i < mv_col_cnt_; ++i) {
    if (i >= SEARCH_TEXT_ROWKEY_CNT
        && i < SEARCH_TEXT_ROWKEY_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
      continue;
    }
    for (int64_t nop = 0; nop < 2; ++nop) {
      ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
          1, "$.title", "database", 42, 1, 2, "pos"));
      if (0 == nop) {
        row.storage_datums_[i].set_null();
      } else {
        row.storage_datums_[i].set_nop();
      }
      ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_row(row));
      EXPECT_FALSE(writer.has_pending_rows_);
    }
  }
  // Allow empty PATH strings, but still reject empty TOKEN and POS strings.
  const int64_t string_cols[] = {SEARCH_TEXT_TOKEN,
      inv_idx_row_desc_.pos_list_col_idx_};
  for (int64_t i = 0; i < ARRAYSIZEOF(string_cols); ++i) {
    ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
        1, "$.title", "database", 42, 1, 2, "pos"));
    row.storage_datums_[string_cols[i]].set_string(ObString());
    ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_row(row));
    EXPECT_FALSE(writer.has_pending_rows_);
  }
  // A rejected row must not leave a partial posting or value behind.
  ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
      1, "$.title", "database", 42, 1, 2, "pos"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_EQ(1u, writer.get_seg_inv_meta().get_stats().total_doc_length_);
  writer.reset();
}

TEST_F(ObInvIdxMacroBlockWriterTest, search_text_accepts_nop_mv_columns_and_empty_path_match)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_search_text_open_env(false /*has_pos_list*/));
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ObDatumRow row;
  // Cover empty and non-empty PATH values with NOP multi-version columns, as produced by compaction.
  const char *paths[] = {"", "$.title"};
  for (int64_t i = 0; i < ARRAYSIZEOF(paths); ++i) {
    ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
        1, paths[i], "database", 42 + i, 1, 2, "pos"));
    for (int64_t j = 0; j < ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(); ++j) {
      row.storage_datums_[SEARCH_TEXT_ROWKEY_CNT + j].set_nop();
    }
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_EQ(2u, writer.get_seg_inv_meta().get_stats().total_doc_length_);
  writer.reset();
}

TEST_F(ObInvIdxMacroBlockWriterTest, search_text_accepts_nop_mv_columns_and_empty_path_phrase)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_search_text_open_env(true /*has_pos_list*/));
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ObDatumRow row;
  // Cover empty and non-empty PATH values with NOP multi-version columns, as produced by compaction.
  const char *paths[] = {"", "$.title"};
  for (int64_t i = 0; i < ARRAYSIZEOF(paths); ++i) {
    ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
        1, paths[i], "database", 42 + i, 1, 2, "pos"));
    for (int64_t j = 0; j < ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(); ++j) {
      row.storage_datums_[SEARCH_TEXT_ROWKEY_CNT + j].set_nop();
    }
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_EQ(2u, writer.get_seg_inv_meta().get_stats().total_doc_length_);
  writer.reset();
}

// Write search text rows grouped by term, then read every logical block back through the
// posting decoder (DOC_ID/TF/DL) and the posting value reader (POSITION_LIST).
static void verify_search_text_write_round_trip(
    ObInvIdxMacroBlockWriterTest &test,
    const bool has_pos_list);

// 10.1.3 search_text_write_round_trip_match / _phrase: three-column terms are written with
// three-column postings, PHRASE adds the position list value block, and the segment
// total_doc_length is ΣTF rather than ΣDL.
TEST_F(ObInvIdxMacroBlockWriterTest, search_text_write_round_trip_match)
{
  verify_search_text_write_round_trip(*this, false /*has_pos_list*/);
}

TEST_F(ObInvIdxMacroBlockWriterTest, search_text_write_round_trip_phrase)
{
  verify_search_text_write_round_trip(*this, true /*has_pos_list*/);
}

static void verify_search_text_write_round_trip(
    ObInvIdxMacroBlockWriterTest &test,
    const bool has_pos_list)
{
  // Three terms: (1,$.title,database), (1,$.title,guide), (2,$.body,oceanbase).
  // ΣTF = 7 and ΣDL = 18, so total_doc_length_ exposes which sum the writer used.
  const SearchTextTestRow rows[] = {
      {1, "$.title", "database", 42, 1, 2, "pos_a"},
      {1, "$.title", "database", 43, 2, 3, "pos_b"},
      {1, "$.title", "guide", 43, 1, 3, "pos_c"},
      {2, "$.body", "oceanbase", 42, 3, 10, "pos_d"}};
  const int64_t term_row_cnts[] = {2, 1, 1};
  const int64_t row_cnt = ARRAYSIZEOF(rows);
  const int64_t term_cnt = ARRAYSIZEOF(term_row_cnts);

  ObSSTablePrivateObjectCleaner cleaner;
  storage::ObAsyncIndexMacroWriterAdapter adapter;
  ASSERT_EQ(OB_SUCCESS, test.prepare_search_text_open_env(has_pos_list));
  ASSERT_EQ(has_pos_list, test.inv_idx_row_desc_.pos_list_col_idx_ >= 0);
  adapter.inv_idx_row_desc_ = test.inv_idx_row_desc_;
  ASSERT_EQ(OB_SUCCESS, test.open_writer(adapter.inv_writer_, cleaner));
  adapter.is_inited_ = true;

  ObDatumRow row;
  for (int64_t i = 0; i < row_cnt; ++i) {
    const SearchTextTestRow &r = rows[i];
    ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, test.inv_idx_row_desc_, test.mv_col_cnt_,
        r.col_idx_, r.path_, r.token_, r.doc_id_, r.token_freq_, r.doc_length_, r.pos_list_));
    ASSERT_EQ(OB_SUCCESS, adapter.append_row(row));
  }
  ObInvIdxMacroBlockWriter &writer = adapter.inv_writer_;
  ASSERT_EQ(SEARCH_TEXT_ROWKEY_CNT - 1, writer.term_col_count_);
  // The last finished term stays pending; earlier finished terms move to micro_term_groups_.
  ASSERT_EQ(1, writer.micro_term_groups_.count());
  const ObInvIdxLogicalBlockGroup &first_group = writer.micro_term_groups_.at(0);
  ASSERT_EQ(SEARCH_TEXT_ROWKEY_CNT - 1, first_group.term_.get_datum_cnt());
  EXPECT_EQ(1u, first_group.term_.datums_[SEARCH_TEXT_COL_IDX].get_uint());
  EXPECT_EQ(ObString("$.title"), first_group.term_.datums_[SEARCH_TEXT_PATH].get_string());
  EXPECT_EQ(ObString("database"), first_group.term_.datums_[SEARCH_TEXT_TOKEN].get_string());
  EXPECT_EQ(2, first_group.doc_count_);
  ASSERT_EQ(row_cnt, adapter.get_row_count());
  ASSERT_EQ(OB_SUCCESS, adapter.close());

  const ObInvIdxStatsMeta &seg_meta = writer.get_seg_inv_meta().get_stats();
  EXPECT_EQ(INV_IDX_TYPE_SEARCH_TEXT, seg_meta.index_type_);
  EXPECT_TRUE(seg_meta.has_relevance_params_);
  EXPECT_EQ(has_pos_list, seg_meta.has_position_list_);
  EXPECT_FALSE(seg_meta.has_nested_key_);
  EXPECT_EQ(42u, seg_meta.min_doc_id_);
  EXPECT_EQ(43u, seg_meta.max_doc_id_);
  EXPECT_EQ(7u, seg_meta.total_doc_length_);
  EXPECT_TRUE(seg_meta.is_valid());

  const char *posting_micro_block = nullptr;
  int64_t posting_micro_block_size = 0;
  const char *posting_value_micro_block = nullptr;
  int64_t posting_value_micro_block_size = 0;
  locate_search_text_micro_blocks(writer, has_pos_list,
      posting_micro_block, posting_micro_block_size,
      posting_value_micro_block, posting_value_micro_block_size);
  ASSERT_FALSE(::testing::Test::HasFatalFailure());

  ObInvIdxMicroBlockReader posting_reader;
  ASSERT_EQ(OB_SUCCESS, posting_reader.init(
      posting_micro_block, posting_micro_block_size, false /*is_posting_value*/));
  ASSERT_EQ(term_cnt, posting_reader.get_block_count());
  ObInvIdxMicroBlockReader posting_value_reader;
  if (has_pos_list) {
    ASSERT_EQ(OB_SUCCESS, posting_value_reader.init(
        posting_value_micro_block, posting_value_micro_block_size, true /*is_posting_value*/));
    ASSERT_EQ(term_cnt, posting_value_reader.get_block_count());
  }

  common::ObArenaAllocator decode_alloc;
  int64_t row_idx = 0;
  for (int64_t b = 0; b < term_cnt; ++b) {
    const char *block_buf = nullptr;
    int64_t block_size = 0;
    ObPostingBlockDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, posting_reader.get_payload(b, block_buf, block_size));
    ASSERT_EQ(OB_SUCCESS, decoder.init(block_buf, block_size, decode_alloc));
    ASSERT_TRUE(decoder.has_relevance_params());
    ASSERT_EQ(term_row_cnts[b], decoder.get_row_count());
    ObPostingValueBlockReader value_block_reader;
    if (has_pos_list) {
      ASSERT_EQ(OB_SUCCESS, posting_value_reader.get_payload(b, block_buf, block_size));
      ASSERT_EQ(OB_SUCCESS, value_block_reader.init(block_buf, block_size));
    }
    for (int64_t r = 0; r < term_row_cnts[b]; ++r, ++row_idx) {
      const SearchTextTestRow &expected = rows[row_idx];
      uint64_t doc_id = 0;
      uint64_t token_freq = 0;
      uint64_t doc_length = 0;
      ASSERT_EQ(OB_SUCCESS, decoder.get_doc_id(r, doc_id));
      ASSERT_EQ(OB_SUCCESS, decoder.get_relevance_params(r, token_freq, doc_length));
      EXPECT_EQ(expected.doc_id_, doc_id);
      EXPECT_EQ(expected.token_freq_, token_freq);
      EXPECT_EQ(expected.doc_length_, doc_length);
      if (has_pos_list) {
        ObDatum pos_list;
        ASSERT_EQ(OB_SUCCESS, value_block_reader.get_position_list(r, pos_list));
        EXPECT_EQ(ObString(expected.pos_list_), pos_list.get_string());
      }
    }
  }
  ASSERT_EQ(row_cnt, row_idx);
  adapter.reset();
}

// Exercise the production SQL-row buffer and multi-version conversion with a real
// inverted writer. Only schema-service/DDL callback setup is replaced by the fixture.
class SearchTextSliceWriterForTest : public storage::ObCsSliceWriter
{
public:
  ~SearchTextSliceWriterForTest() { storage_slice_writer_ = nullptr; }

  int prepare(ObInvIdxMacroBlockWriterTest &test, storage::ObAsyncIndexMacroWriterAdapter &adapter)
  {
    int ret = OB_SUCCESS;
    storage::ObWriteMacroParam &param = writer_param_;
    param.ls_id_ = test.mock_ls_id_;
    param.tablet_id_ = test.mock_tablet_id_;
    param.tenant_data_version_ = DATA_VERSION_4_6_0_0;
    param.schema_version_ = 1;
    param.slice_idx_ = 0;
    param.snapshot_version_ = 10000;
    param.direct_load_type_ = storage::DIRECT_LOAD_DDL;
    param.task_id_ = 1;
    param.ddl_table_schema_.table_item_.is_index_table_ = true;
    param.ddl_table_schema_.table_item_.rowkey_column_num_ = SEARCH_TEXT_ROWKEY_CNT;
    param.ddl_table_schema_.table_item_.index_type_ = test.table_schema_.get_index_type();
    const auto &columns = test.data_store_desc_.get_full_stored_col_descs();
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      storage::ObColumnSchemaItem item;
      item.is_valid_ = true;
      item.col_type_ = columns.at(i).col_type_;
      item.is_rowkey_column_ = i < SEARCH_TEXT_ROWKEY_CNT;
      item.is_nullable_ = true;
      const auto *column = test.table_schema_.get_column_schema(columns.at(i).col_id_);
      if (nullptr != column) {
        item.col_accuracy_ = column->get_accuracy();
      }
      if(OB_FAIL(param.ddl_table_schema_.column_items_.push_back(item))) {
        LOG_WARN("fail to push back column item", K(ret), K(i));
      }
    }
    rowkey_column_count_ = SEARCH_TEXT_ROWKEY_CNT;
    sql_column_count_ = columns.count() - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    param.tablet_param_.storage_schema_ = &storage_schema_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(storage_schema_.init(test.allocator_, test.table_schema_,
                                          lib::Worker::CompatMode::MYSQL))) {
    } else if (OB_FAIL(current_row_.init(columns.count()))) {
    } else if (OB_FAIL(init_row_buffer())) {
    } else {
      storage_slice_writer_ = &adapter;
      is_inited_ = true;
    }
    return ret;
  }
private:
  storage::ObStorageSchema storage_schema_;
};

static void verify_search_text_generated_rows_round_trip(
    ObInvIdxMacroBlockWriterTest &test,
    const bool has_pos_list)
{
  share::ObSearchIndexModeConfig mode;
  mode.mode_ = has_pos_list ? share::ObSearchIndexModeConfig::FULLTEXT_PHRASE
                           : share::ObSearchIndexModeConfig::FULLTEXT_MATCH;
  mode.filter_ = ObString("INCLUDE_TYPES=11json_string");
  mode.parser_name_ = ObString("space.1");
  mode.parser_properties_ = ObString("{\"min_token_size\":3,\"max_token_size\":84}");
  share::ObSearchIndexConfig config;
  ObSqlString comment;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, comment));
  ObSEArray<int32_t, 1> column_ids;
  ObSEArray<ObObjMeta, 1> column_metas;
  ObSEArray<int64_t, 2> projector;
  ObSEArray<ObString, 1> comments;
  ObObjMeta json_meta;
  json_meta.set_json();
  ASSERT_EQ(OB_SUCCESS, column_ids.push_back(3));
  ASSERT_EQ(OB_SUCCESS, column_metas.push_back(json_meta));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(0));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(1));
  ASSERT_EQ(OB_SUCCESS, comments.push_back(comment.string()));
  share::ObSearchIndexRowGenerator generator;
  ASSERT_EQ(OB_SUCCESS, generator.init(column_ids, column_metas, projector, comments,
      false /*rowkey_only*/, nullptr, has_pos_list ? share::SEARCH_TEXT_PHRASE
                                                 : share::SEARCH_TEXT_MATCH));

  const char *documents[] = {
      "{\"tags\":[\"alpha alpha\",\"beta\",\"alpha\"]}",
      "{\"tags\":[\"beta alpha\",\"\",\"alpha beta\"]}",
      "{\"tags\":[\"a\",\"\",null,true]}"};
  const uint64_t doc_ids[] = {43, 42, 41}; // force a DOC_ID reorder within each term
  const int64_t output_counts[] = {2, 2, 0};
  const int64_t row_count = 4;
  const int64_t column_count = has_pos_list ? SEARCH_TEXT_COL_CNT_WITH_POS
                                           : SEARCH_TEXT_COL_CNT_WITHOUT_POS;
  common::ObArenaAllocator input_allocator;
  common::ObArenaAllocator held_allocator;
  ObDatumRow input;
  ObDatumRow held_rows[row_count];
  ObDatumRow *sorted_rows[row_count];
  ASSERT_EQ(OB_SUCCESS, input.init(input_allocator, 2));
  int64_t held_count = 0;
  for (int64_t d = 0; d < ARRAYSIZEOF(documents); ++d) {
    ObIJsonBase *json = nullptr;
    ObString binary;
    ObDatum with_header;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&input_allocator,
        ObString(documents[d]), ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, json));
    ASSERT_EQ(OB_SUCCESS, json->get_raw_binary(binary));
    ASSERT_EQ(OB_SUCCESS, sql::ObTextStringHelper::string_to_templob_result(
        ObJsonType, input_allocator, binary, with_header));
    input.storage_datums_[0].shallow_copy_from_datum(with_header);
    input.storage_datums_[1].set_uint(doc_ids[d]);
    ObDatumRow *generated_rows = nullptr;
    int64_t generated_count = 0;
    ASSERT_EQ(OB_SUCCESS, generator.generate_rows(input, generated_rows, generated_count));
    ASSERT_EQ(output_counts[d], generated_count);
    if (0 == generated_count) {
      EXPECT_EQ(nullptr, generated_rows);
    } else {
      ASSERT_NE(nullptr, generated_rows);
    }
    for (int64_t r = 0; r < generated_count; ++r, ++held_count) {
      ASSERT_LT(held_count, row_count);
      ASSERT_EQ(column_count, generated_rows[r].get_column_count());
      // generate_rows() reuses its allocator on every call, including the no-token document.
      ASSERT_EQ(OB_SUCCESS, held_rows[held_count].init(held_allocator, column_count));
      ASSERT_EQ(OB_SUCCESS, held_rows[held_count].deep_copy(generated_rows[r], held_allocator));
      sorted_rows[held_count] = &held_rows[held_count];
    }
  }
  ASSERT_EQ(row_count, held_count);
  generator.reset(); // all rows and variable-length fields must now be independently owned
  EXPECT_EQ(43u, held_rows[0].storage_datums_[SEARCH_TEXT_DOC_ID].get_uint());
  EXPECT_EQ(42u, held_rows[2].storage_datums_[SEARCH_TEXT_DOC_ID].get_uint());
  std::sort(sorted_rows, sorted_rows + row_count, [](const ObDatumRow *left, const ObDatumRow *right) {
    const ObStorageDatum *lhs = left->storage_datums_;
    const ObStorageDatum *rhs = right->storage_datums_;
    bool less = false;
    if (lhs[SEARCH_TEXT_COL_IDX].get_uint() != rhs[SEARCH_TEXT_COL_IDX].get_uint()) {
      less = lhs[SEARCH_TEXT_COL_IDX].get_uint() < rhs[SEARCH_TEXT_COL_IDX].get_uint();
    } else {
      int cmp = 0;
      for (int64_t i = SEARCH_TEXT_PATH; 0 == cmp && i <= SEARCH_TEXT_TOKEN; ++i) {
        cmp = lhs[i].get_string().compare(rhs[i].get_string());
      }
      less = 0 != cmp ? cmp < 0 : lhs[SEARCH_TEXT_DOC_ID].get_uint() < rhs[SEARCH_TEXT_DOC_ID].get_uint();
    }
    return less;
  });

  // Expected scalar values come from the source documents, not a prepared posting payload.
  const uint64_t expected_doc_ids[] = {42, 43, 42, 43};
  const uint64_t expected_token_freqs[] = {2, 3, 2, 1};
  const int64_t expected_positions[][3] = {{1, 102, 0}, {0, 1, 203}, {0, 103, 0}, {102, 0, 0}};
  // JSON string keys contain the token bytes followed by a two-byte terminator.
  const ObString expected_tokens[] = {ObString(7, "alpha\0\0"), ObString(6, "beta\0\0")};

  ObSSTablePrivateObjectCleaner cleaner;
  storage::ObAsyncIndexMacroWriterAdapter adapter;
  ASSERT_EQ(OB_SUCCESS, test.prepare_search_text_open_env(has_pos_list));
  adapter.inv_idx_row_desc_ = test.inv_idx_row_desc_;
  ASSERT_EQ(OB_SUCCESS, test.open_writer(adapter.inv_writer_, cleaner));
  adapter.is_inited_ = true;
  SearchTextSliceWriterForTest slice_writer;
  ASSERT_EQ(OB_SUCCESS, slice_writer.prepare(test, adapter));
  for (int64_t r = 0; r < row_count; ++r) {
    const ObDatumRow &row = *sorted_rows[r];
    EXPECT_EQ(3u, row.storage_datums_[SEARCH_TEXT_COL_IDX].get_uint());
    EXPECT_EQ(expected_tokens[r / 2], row.storage_datums_[SEARCH_TEXT_TOKEN].get_string());
    EXPECT_EQ(expected_doc_ids[r], row.storage_datums_[SEARCH_TEXT_DOC_ID].get_uint());
    EXPECT_EQ(expected_token_freqs[r], row.storage_datums_[4].get_uint());
    EXPECT_EQ(4u, row.storage_datums_[5].get_uint());
    EXPECT_EQ(sorted_rows[0]->storage_datums_[SEARCH_TEXT_PATH].get_string(),
        row.storage_datums_[SEARCH_TEXT_PATH].get_string());
    ObSEArray<ObDatum *, SEARCH_TEXT_COL_CNT_WITH_POS> datums;
    for (int64_t c = 0; c < column_count; ++c) {
      ASSERT_EQ(OB_SUCCESS, datums.push_back(&sorted_rows[r]->storage_datums_[c]));
    }
    ASSERT_EQ(OB_SUCCESS, slice_writer.append_current_row(datums));
  }
  // Buffered rows must own their bytes before the upstream buffers are reused.
  for (int64_t r = 0; r < row_count; ++r) {
    held_rows[r].reset();
  }
  held_allocator.reset();
  ASSERT_EQ(0, adapter.get_row_count());
  int64_t buffered_count = 0;
  ASSERT_EQ(OB_SUCCESS, slice_writer.get_row_count(buffered_count));
  ASSERT_EQ(row_count, buffered_count);
  ASSERT_EQ(OB_SUCCESS, slice_writer.close());
  ASSERT_EQ(row_count, adapter.get_row_count());
  ObInvIdxMacroBlockWriter &writer = adapter.inv_writer_;
  EXPECT_EQ(42u, writer.get_seg_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(43u, writer.get_seg_inv_meta().get_stats().max_doc_id_);
  EXPECT_EQ(8u, writer.get_seg_inv_meta().get_stats().total_doc_length_); // sum(TF), not sum(DL)
  EXPECT_EQ(has_pos_list, writer.get_seg_inv_meta().get_stats().has_position_list_);

  const char *posting_micro_block = nullptr;
  int64_t posting_micro_block_size = 0;
  const char *posting_value_micro_block = nullptr;
  int64_t posting_value_micro_block_size = 0;
  locate_search_text_micro_blocks(writer, has_pos_list,
      posting_micro_block, posting_micro_block_size,
      posting_value_micro_block, posting_value_micro_block_size);
  ASSERT_FALSE(::testing::Test::HasFatalFailure());
  ObInvIdxMicroBlockReader posting_reader;
  ASSERT_EQ(OB_SUCCESS, posting_reader.init(
      posting_micro_block, posting_micro_block_size, false /*is_posting_value*/));
  ASSERT_EQ(2, posting_reader.get_block_count());
  ObInvIdxMicroBlockReader posting_value_reader;
  if (has_pos_list) {
    ASSERT_EQ(OB_SUCCESS, posting_value_reader.init(
        posting_value_micro_block, posting_value_micro_block_size, true /*is_posting_value*/));
    ASSERT_EQ(2, posting_value_reader.get_block_count());
  }
  common::ObArenaAllocator decode_allocator;
  for (int64_t b = 0; b < 2; ++b) {
    const char *payload = nullptr;
    int64_t payload_size = 0;
    ObPostingBlockDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, posting_reader.get_payload(b, payload, payload_size));
    ASSERT_EQ(OB_SUCCESS, decoder.init(payload, payload_size, decode_allocator));
    ASSERT_TRUE(decoder.has_relevance_params());
    ASSERT_EQ(2, decoder.get_row_count());
    ObPostingValueBlockReader value_reader;
    if (has_pos_list) {
      ASSERT_EQ(OB_SUCCESS, posting_value_reader.get_payload(b, payload, payload_size));
      ASSERT_EQ(OB_SUCCESS, value_reader.init(payload, payload_size));
    }
    for (int64_t r = 0; r < 2; ++r) {
      const int64_t row_idx = b * 2 + r;
      uint64_t doc_id = 0;
      uint64_t token_freq = 0;
      uint64_t doc_length = 0;
      ASSERT_EQ(OB_SUCCESS, decoder.get_doc_id(r, doc_id));
      ASSERT_EQ(OB_SUCCESS, decoder.get_relevance_params(r, token_freq, doc_length));
      EXPECT_EQ(expected_doc_ids[row_idx], doc_id);
      EXPECT_EQ(expected_token_freqs[row_idx], token_freq);
      EXPECT_EQ(4u, doc_length);
      if (has_pos_list) {
        ObDatum positions;
        ASSERT_EQ(OB_SUCCESS, value_reader.get_position_list(r, positions));
        const ObString stored = positions.get_string();
        // Decode persisted positions independently after the input buffers were released.
        share::ObFTSPositionListStore position_store;
        ObSEArray<int64_t, 8> decoded_positions;
        ASSERT_EQ(OB_SUCCESS, position_store.deserialize_and_decode(
            stored.ptr(), stored.length(), decoded_positions));
        ASSERT_EQ(expected_token_freqs[row_idx], decoded_positions.count());
        for (int64_t p = 0; p < decoded_positions.count(); ++p) {
          EXPECT_EQ(expected_positions[row_idx][p], decoded_positions.at(p));
        }
      }
    }
  }
  adapter.reset();
}

TEST_F(ObInvIdxMacroBlockWriterTest, search_text_generated_rows_round_trip_match)
{
  verify_search_text_generated_rows_round_trip(*this, false /*has_pos_list*/);
}

TEST_F(ObInvIdxMacroBlockWriterTest, search_text_generated_rows_round_trip_phrase)
{
  verify_search_text_generated_rows_round_trip(*this, true /*has_pos_list*/);
}

#ifndef OB_BUILD_PACKAGE
static void verify_search_text_keyword_trailing_space(
    ObInvIdxMacroBlockWriterTest &test, const bool has_pos_list)
{
  share::ObSearchIndexModeConfig mode;
  mode.mode_ = has_pos_list ? share::ObSearchIndexModeConfig::FULLTEXT_PHRASE
                           : share::ObSearchIndexModeConfig::FULLTEXT_MATCH;
  mode.filter_ = ObString("INCLUDE_TYPES=11json_string");
  mode.parser_name_ = ObString("analyzer.1");
  mode.parser_properties_ = ObString(
      R"({"analyzer":{"a":{"type":"custom","tokenizer":"keyword"}}})");
  share::ObSearchIndexConfig config;
  ObSqlString comment;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, comment));
  ObSEArray<int32_t, 1> column_ids;
  ObSEArray<ObObjMeta, 1> column_metas;
  ObSEArray<int64_t, 2> projector;
  ObSEArray<ObString, 1> comments;
  ObObjMeta json_meta;
  json_meta.set_json();
  ASSERT_EQ(OB_SUCCESS, column_ids.push_back(3));
  ASSERT_EQ(OB_SUCCESS, column_metas.push_back(json_meta));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(0));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(1));
  ASSERT_EQ(OB_SUCCESS, comments.push_back(comment.string()));
  share::ObSearchIndexRowGenerator generator;
  ASSERT_EQ(OB_SUCCESS, generator.init(column_ids, column_metas, projector, comments,
      false /*rowkey_only*/, nullptr, has_pos_list ? share::SEARCH_TEXT_PHRASE
                                                 : share::SEARCH_TEXT_MATCH));
  ObSSTablePrivateObjectCleaner cleaner;
  storage::ObAsyncIndexMacroWriterAdapter adapter;
  ASSERT_EQ(OB_SUCCESS, test.prepare_search_text_open_env(has_pos_list));
  adapter.inv_idx_row_desc_ = test.inv_idx_row_desc_;
  ASSERT_EQ(OB_SUCCESS, test.open_writer(adapter.inv_writer_, cleaner));
  adapter.is_inited_ = true;
  SearchTextSliceWriterForTest slice_writer;
  ASSERT_EQ(OB_SUCCESS, slice_writer.prepare(test, adapter));
  common::ObArenaAllocator input_allocator;
  ObDatumRow input;
  ASSERT_EQ(OB_SUCCESS, input.init(input_allocator, 2));
  const char *documents[] = {R"(["foo","foo ","foo  ","foo"])",
                             R"(["foo  ","foo ","foo","foo"])"};
  const ObString tokens[] = {ObString(5, "foo\0\0"), ObString(6, "foo \0\0"), ObString(7, "foo  \0\0")};
  // Feed postings in storage key order: TOKEN, then DOC_ID.
  for (int64_t t = 0; t < ARRAYSIZEOF(tokens); ++t) {
    for (int64_t d = 0; d < ARRAYSIZEOF(documents); ++d) {
      ObIJsonBase *json = nullptr;
      ObString binary;
      ObDatum with_header;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&input_allocator,
          ObString(documents[d]), ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, json));
      ASSERT_EQ(OB_SUCCESS, json->get_raw_binary(binary));
      ASSERT_EQ(OB_SUCCESS, sql::ObTextStringHelper::string_to_templob_result(
          ObJsonType, input_allocator, binary, with_header));
      input.storage_datums_[0].shallow_copy_from_datum(with_header);
      input.storage_datums_[1].set_uint(42 + d);
      ObDatumRow *rows = nullptr;
      int64_t count = 0;
      ASSERT_EQ(OB_SUCCESS, generator.generate_rows(input, rows, count));
      ASSERT_EQ(3, count);
      int64_t row_idx = -1;
      for (int64_t i = 0; i < count; ++i) {
        if (rows[i].storage_datums_[SEARCH_TEXT_TOKEN].get_string() == tokens[t]) {
          row_idx = i;
        }
      }
      ASSERT_GE(row_idx, 0);
      EXPECT_EQ(t == 0 ? 2u : 1u, rows[row_idx].storage_datums_[4].get_uint());
      EXPECT_EQ(4u, rows[row_idx].storage_datums_[5].get_uint());
      ObSEArray<ObDatum *, SEARCH_TEXT_COL_CNT_WITH_POS> datums;
      for (int64_t c = 0; c < rows[row_idx].get_column_count(); ++c) {
        ASSERT_EQ(OB_SUCCESS, datums.push_back(&rows[row_idx].storage_datums_[c]));
      }
      ASSERT_EQ(OB_SUCCESS, slice_writer.append_current_row(datums));
    }
  }
  generator.reset();
  ASSERT_EQ(OB_SUCCESS, slice_writer.close());
  ASSERT_EQ(6, adapter.get_row_count());
  const ObDatumRowkey &stored_key = adapter.inv_writer_.index_block_builder_.macro_meta_.end_key_;
  ASSERT_TRUE(stored_key.is_valid());
  ASSERT_GT(stored_key.get_datum_cnt(), SEARCH_TEXT_TOKEN);
  EXPECT_EQ(tokens[2], stored_key.get_datum(SEARCH_TEXT_TOKEN).get_string());
  const char *posting_block = nullptr;
  int64_t posting_size = 0;
  const char *value_block = nullptr;
  int64_t value_size = 0;
  locate_search_text_micro_blocks(adapter.inv_writer_, has_pos_list,
      posting_block, posting_size, value_block, value_size);
  ASSERT_FALSE(::testing::Test::HasFatalFailure());
  ObInvIdxMicroBlockReader posting_reader;
  ASSERT_EQ(OB_SUCCESS, posting_reader.init(posting_block, posting_size, false));
  ASSERT_EQ(3, posting_reader.get_block_count()); // Trailing spaces distinguish terms.
  ObInvIdxMicroBlockReader value_reader;
  if (has_pos_list) {
    ASSERT_EQ(OB_SUCCESS, value_reader.init(value_block, value_size, true));
    ASSERT_EQ(3, value_reader.get_block_count());
  }
  for (int64_t t = 0; t < ARRAYSIZEOF(tokens); ++t) {
    const char *payload = nullptr;
    int64_t payload_size = 0;
    ASSERT_EQ(OB_SUCCESS, posting_reader.get_payload(t, payload, payload_size));
    ObPostingBlockDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, decoder.init(payload, payload_size, input_allocator));
    ASSERT_EQ(2, decoder.get_row_count());
    ObPostingValueBlockReader positions_reader;
    if (has_pos_list) {
      ASSERT_EQ(OB_SUCCESS, value_reader.get_payload(t, payload, payload_size));
      ASSERT_EQ(OB_SUCCESS, positions_reader.init(payload, payload_size));
    }
    for (int64_t d = 0; d < 2; ++d) {
      uint64_t doc_id = 0;
      uint64_t tf = 0;
      uint64_t dl = 0;
      ASSERT_EQ(OB_SUCCESS, decoder.get_doc_id(d, doc_id));
      ASSERT_EQ(OB_SUCCESS, decoder.get_relevance_params(d, tf, dl));
      EXPECT_EQ(42u + d, doc_id);
      EXPECT_EQ(t == 0 ? 2u : 1u, tf);
      EXPECT_EQ(4u, dl);
      if (has_pos_list) {
        ObDatum positions;
        ASSERT_EQ(OB_SUCCESS, positions_reader.get_position_list(d, positions));
        const ObString stored = positions.get_string();
        share::ObFTSPositionListStore store;
        ObSEArray<int64_t, 2> decoded;
        ASSERT_EQ(OB_SUCCESS, store.deserialize_and_decode(stored.ptr(), stored.length(), decoded));
        ASSERT_EQ(t == 0 ? 2 : 1, decoded.count());
        EXPECT_EQ(101 * (d == 0 ? t : 2 - t), decoded.at(0));
        if (t == 0) { EXPECT_EQ(303, decoded.at(1)); }
      }
    }
  }
  adapter.reset();
}

TEST_F(ObInvIdxMacroBlockWriterTest, search_text_keyword_trailing_space_match)
{
  verify_search_text_keyword_trailing_space(*this, false);
}

TEST_F(ObInvIdxMacroBlockWriterTest, search_text_keyword_trailing_space_phrase)
{
  verify_search_text_keyword_trailing_space(*this, true);
}
#endif

// 10.1.4 search_text_logical_block_split: a change in COL_IDX alone or PATH alone starts a new
// term group, a term longer than the logical block capacity is split at 256 docs, and every
// field still lands in the same posting micro block (micro/macro blocks are not cut by field).
TEST_F(ObInvIdxMacroBlockWriterTest, search_text_logical_block_split)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_search_text_open_env(false /*has_pos_list*/));
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  // Same PATH/TOKEN, only COL_IDX changes between the first two terms; only PATH changes
  // between the second and the third.
  ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
      1, "$.a", "db", 1, 1, 4, nullptr));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
      2, "$.a", "db", 1, 1, 4, nullptr));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
      2, "$.b", "db", 1, 1, 4, nullptr));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  for (int64_t i = 1; i <= 300; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
        2, "$.b", "zz", static_cast<uint64_t>(i), 1, 4, nullptr));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
      3, "$.c", "end", 1, 1, 4, nullptr));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  struct ExpectedTerm { uint64_t col_idx_; const char *path_; const char *token_; };
  const ExpectedTerm expected_terms[] = {{1, "$.a", "db"}, {2, "$.a", "db"}, {2, "$.b", "db"}};
  ASSERT_EQ(ARRAYSIZEOF(expected_terms), writer.micro_term_groups_.count());
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_terms); ++i) {
    const ObInvIdxLogicalBlockGroup &group = writer.micro_term_groups_.at(i);
    ASSERT_EQ(SEARCH_TEXT_ROWKEY_CNT - 1, group.term_.get_datum_cnt());
    EXPECT_EQ(expected_terms[i].col_idx_, group.term_.datums_[SEARCH_TEXT_COL_IDX].get_uint());
    EXPECT_EQ(ObString(expected_terms[i].path_),
        group.term_.datums_[SEARCH_TEXT_PATH].get_string());
    EXPECT_EQ(ObString(expected_terms[i].token_),
        group.term_.datums_[SEARCH_TEXT_TOKEN].get_string());
    EXPECT_EQ(1, group.doc_count_);
    EXPECT_EQ(1, group.logical_block_metas_.count());
  }
  const ObInvIdxLogicalBlockGroup &capacity_group = writer.pending_term_group_;
  EXPECT_EQ(ObString("zz"), capacity_group.term_.datums_[SEARCH_TEXT_TOKEN].get_string());
  ASSERT_EQ(2, capacity_group.logical_block_metas_.count());
  EXPECT_EQ(300, capacity_group.doc_count_);
  EXPECT_EQ(256, capacity_group.logical_block_metas_.at(0).doc_count_);
  EXPECT_EQ(256ul, capacity_group.logical_block_metas_.at(0).max_doc_id_);
  EXPECT_EQ(44, capacity_group.logical_block_metas_.at(1).doc_count_);
  EXPECT_EQ(257ul, capacity_group.logical_block_metas_.at(1).min_doc_id_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
  const char *posting_micro_block = nullptr;
  int64_t posting_micro_block_size = 0;
  const char *posting_value_micro_block = nullptr;
  int64_t posting_value_micro_block_size = 0;
  locate_search_text_micro_blocks(writer, false /*has_pos_list*/,
      posting_micro_block, posting_micro_block_size,
      posting_value_micro_block, posting_value_micro_block_size);
  ASSERT_FALSE(HasFatalFailure());
  ObInvIdxMicroBlockReader posting_reader;
  ASSERT_EQ(OB_SUCCESS, posting_reader.init(
      posting_micro_block, posting_micro_block_size, false /*is_posting_value*/));
  // 3 single-doc terms + 2 capacity blocks + the trailing term.
  EXPECT_EQ(6, posting_reader.get_block_count());
  EXPECT_EQ(304u, writer.get_seg_inv_meta().get_stats().total_doc_length_);
  writer.reset();
}

// 10.1.5 search_text_flat_cache_transform: the posting micro block written for search text is
// transformed into the FlatCache layout and the cached view returns the persisted DOC_ID/TF/DL.
TEST_F(ObInvIdxMacroBlockWriterTest, search_text_flat_cache_transform)
{
  const SearchTextTestRow rows[] = {
      {1, "$.title", "database", 7, 2, 9, nullptr},
      {1, "$.title", "database", 11, 5, 20, nullptr},
      {2, "$.body", "oceanbase", 3, 1, 6, nullptr}};
  const int64_t term_row_cnts[] = {2, 1};
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_search_text_open_env(false /*has_pos_list*/));
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ObDatumRow row;
  for (int64_t i = 0; i < ARRAYSIZEOF(rows); ++i) {
    const SearchTextTestRow &r = rows[i];
    ASSERT_EQ(OB_SUCCESS, make_search_text_row(row, inv_idx_row_desc_, mv_col_cnt_,
        r.col_idx_, r.path_, r.token_, r.doc_id_, r.token_freq_, r.doc_length_, r.pos_list_));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());

  const char *posting_micro_block = nullptr;
  int64_t posting_micro_block_size = 0;
  const char *posting_value_micro_block = nullptr;
  int64_t posting_value_micro_block_size = 0;
  locate_search_text_micro_blocks(writer, false /*has_pos_list*/,
      posting_micro_block, posting_micro_block_size,
      posting_value_micro_block, posting_value_micro_block_size);
  ASSERT_FALSE(HasFatalFailure());

  ObMicroBlockHeader header;
  int64_t header_pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.deserialize(posting_micro_block, posting_micro_block_size, header_pos));
  ObInvIdxMicroBlockTransformer transformer;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  int64_t transformed_size = 0;
  ASSERT_EQ(OB_SUCCESS, transformer.init(header, posting_micro_block, posting_micro_block_size,
      des_meta, mb_reader, allocator_));
  ASSERT_EQ(OB_SUCCESS, transformer.get_buf_size(transformed_size));
  char *transformed_buf = static_cast<char *>(allocator_.alloc(transformed_size));
  ASSERT_NE(nullptr, transformed_buf);
  ASSERT_EQ(OB_SUCCESS, transformer.transform(transformed_buf, transformed_size));

  ObFlatCacheReader view_reader;
  ASSERT_EQ(OB_SUCCESS, view_reader.init(transformed_buf, transformed_size));
  ASSERT_EQ(ARRAYSIZEOF(term_row_cnts), view_reader.get_block_count());
  int64_t row_idx = 0;
  for (int64_t b = 0; b < ARRAYSIZEOF(term_row_cnts); ++b) {
    ObPostingBlockData view;
    ASSERT_EQ(OB_SUCCESS, view_reader.get_posting_block_data(b, view));
    ASSERT_TRUE(view.has_relevance_params());
    ASSERT_EQ(term_row_cnts[b], view.get_row_count());
    for (int64_t r = 0; r < term_row_cnts[b]; ++r, ++row_idx) {
      uint64_t doc_id = 0;
      uint64_t token_freq = 0;
      uint64_t doc_length = 0;
      ASSERT_EQ(OB_SUCCESS, view.get_doc_id(r, doc_id));
      ASSERT_EQ(OB_SUCCESS, view.get_relevance_params(r, token_freq, doc_length));
      EXPECT_EQ(rows[row_idx].doc_id_, doc_id);
      EXPECT_EQ(rows[row_idx].token_freq_, token_freq);
      EXPECT_EQ(rows[row_idx].doc_length_, doc_length);
    }
  }
  writer.reset();
}

// 10.2 search_index_append_row: append rows with multi-col term + nested_key,
// verify the write path produces one macro block with correct term group layout.
TEST_F(ObInvIdxMacroBlockWriterTest, search_index_append_row)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_search_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ASSERT_TRUE(writer.is_opened());

  ObDatumRow row;
  for (int64_t i = 1; i <= 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_search_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "1", "$.a", "val_a", static_cast<uint64_t>(i), "nk_a"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  // term switch: flush pending logical block for term ("1","$.a","val_a")
  ASSERT_EQ(OB_SUCCESS, make_search_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "2", "$.b", "val_b", 11, "nk_b"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  EXPECT_TRUE(writer.micro_term_groups_.empty());
  const ObInvIdxLogicalBlockGroup &group = writer.pending_term_group_;
  EXPECT_EQ(1, group.logical_block_metas_.count());
  EXPECT_EQ(10, group.doc_count_);
  EXPECT_EQ(10ul, group.max_doc_id_);
  EXPECT_EQ(10, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_EQ(1, writer.posting_encoder_.get_doc_count());
  EXPECT_TRUE(writer.has_pending_rows_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_);
  EXPECT_EQ(1, writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_->get_macro_block_count());
  writer.reset();
}

// 10.3 search_index_nested_key_list_macro_round_trip: persist an ordered nested-key list and a
// NULL list through the physical posting-value micro block, then read both logical blocks back.
TEST_F(ObInvIdxMacroBlockWriterTest, search_index_nested_key_list_macro_round_trip)
{
  const char key_bytes[][4] = {
      {0x00, 0x00, 0x00, 0x01},
      {0x00, 0x00, 0x00, 0x02},
      {0x00, 0x00, 0x00, 0x03}};
  const char expected_nested_key_list_bytes[] = {
      1, 0, 0, 0, 3, 4,
      0x00, 0x00, 0x00, 0x01,
      0x00, 0x00, 0x00, 0x02,
      0x00, 0x00, 0x00, 0x03};
  const ObString expected_nested_key_list(
      static_cast<int32_t>(sizeof(expected_nested_key_list_bytes)),
      expected_nested_key_list_bytes);

  ObSSTablePrivateObjectCleaner cleaner;
  storage::ObAsyncIndexMacroWriterAdapter adapter;
  ASSERT_EQ(OB_SUCCESS, prepare_search_open_env());
  adapter.inv_idx_row_desc_ = inv_idx_row_desc_;
  ASSERT_EQ(OB_SUCCESS, open_writer(adapter.inv_writer_, cleaner));
  adapter.is_inited_ = true;

  ObDatumRow row;
  ObDatum scalar_nested_key;
  ObString scalar_nested_key_string;
  for (int64_t i = 0; i < ARRAYSIZEOF(key_bytes); ++i) {
    scalar_nested_key_string.assign_ptr(
        key_bytes[i], static_cast<int32_t>(sizeof(key_bytes[i])));
    scalar_nested_key.set_string(scalar_nested_key_string);
    ASSERT_EQ(OB_SUCCESS, make_search_idx_row_with_nested_key(
        row, inv_idx_row_desc_, mv_col_cnt_, "1", "$.a", "val_a", 1, scalar_nested_key));
    ASSERT_EQ(OB_SUCCESS, adapter.append_row(row));
  }

  ObDatum null_nested_key;
  null_nested_key.set_null();
  ASSERT_EQ(OB_SUCCESS, make_search_idx_row_with_nested_key(
      row, inv_idx_row_desc_, mv_col_cnt_, "1", "$.a", "val_a", 2, null_nested_key));
  ASSERT_EQ(OB_SUCCESS, adapter.append_row(row));

  for (int64_t i = 0; i < ARRAYSIZEOF(key_bytes); ++i) {
    scalar_nested_key_string.assign_ptr(
        key_bytes[i], static_cast<int32_t>(sizeof(key_bytes[i])));
    scalar_nested_key.set_string(scalar_nested_key_string);
    ASSERT_EQ(OB_SUCCESS, make_search_idx_row_with_nested_key(
        row, inv_idx_row_desc_, mv_col_cnt_, "2", "$.b", "val_b", 3, scalar_nested_key));
    ASSERT_EQ(OB_SUCCESS, adapter.append_row(row));
  }
  ASSERT_EQ(7, adapter.get_row_count());
  ASSERT_EQ(OB_SUCCESS, adapter.close());

  ObInvIdxMacroBlockWriter &writer = adapter.inv_writer_;
  ASSERT_EQ(2, writer.macro_block_.get_micro_block_count());
  const char *posting_micro_block = writer.macro_block_.get_micro_block_data_ptr();
  const int64_t micro_block_data_size = writer.macro_block_.get_micro_block_data_size();
  ObMicroBlockHeader posting_header;
  int64_t header_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      posting_header.deserialize(posting_micro_block, micro_block_data_size, header_pos));
  ASSERT_EQ(MICRO_BLOCK_INV_IDX_POSTING,
      static_cast<ObMicroBlockInvertedIndexType>(posting_header.inverted_index_type_));
  const int64_t posting_micro_block_size =
      posting_header.header_size_ + posting_header.data_zlength_;
  ASSERT_LT(posting_micro_block_size, micro_block_data_size);

  const char *posting_value_micro_block = posting_micro_block + posting_micro_block_size;
  const int64_t posting_value_remain_size = micro_block_data_size - posting_micro_block_size;
  ObMicroBlockHeader posting_value_header;
  header_pos = 0;
  ASSERT_EQ(OB_SUCCESS, posting_value_header.deserialize(
      posting_value_micro_block, posting_value_remain_size, header_pos));
  ASSERT_EQ(MICRO_BLOCK_INV_IDX_POSTING_VALUE,
      static_cast<ObMicroBlockInvertedIndexType>(posting_value_header.inverted_index_type_));
  const int64_t posting_value_micro_block_size =
      posting_value_header.header_size_ + posting_value_header.data_zlength_;
  ASSERT_LE(posting_value_micro_block_size, posting_value_remain_size);

  ObInvIdxMicroBlockReader micro_block_reader;
  ASSERT_EQ(OB_SUCCESS, micro_block_reader.init(
      posting_value_micro_block, posting_value_micro_block_size, true /*is_posting_value*/));
  ASSERT_EQ(2, micro_block_reader.get_block_count());

  const char *posting_value_block = nullptr;
  int64_t posting_value_block_size = 0;
  ObPostingValueBlockReader posting_value_reader;
  ObDatum persisted_nested_key_list;
  ASSERT_EQ(OB_SUCCESS,
      micro_block_reader.get_payload(0, posting_value_block, posting_value_block_size));
  ASSERT_EQ(OB_SUCCESS, posting_value_reader.init(posting_value_block, posting_value_block_size));
  ASSERT_EQ(OB_SUCCESS, posting_value_reader.get_nested_key(0, persisted_nested_key_list));
  ASSERT_EQ(expected_nested_key_list, persisted_nested_key_list.get_string());

  share::ObSearchIndexNestedKeyListReader nested_key_list_reader;
  ObDatum decoded_nested_key;
  ASSERT_EQ(OB_SUCCESS, nested_key_list_reader.init(persisted_nested_key_list));
  ASSERT_EQ(ARRAYSIZEOF(key_bytes), nested_key_list_reader.get_count());
  ASSERT_EQ(OB_SUCCESS, nested_key_list_reader.first(decoded_nested_key));
  for (int64_t i = 0; i < ARRAYSIZEOF(key_bytes); ++i) {
    if (i > 0) {
      ASSERT_EQ(OB_SUCCESS, nested_key_list_reader.next(decoded_nested_key));
    }
    ASSERT_EQ(sizeof(key_bytes[i]), decoded_nested_key.len_);
    ASSERT_EQ(0, MEMCMP(key_bytes[i], decoded_nested_key.ptr_, sizeof(key_bytes[i])));
  }
  ASSERT_EQ(OB_ITER_END, nested_key_list_reader.next(decoded_nested_key));

  ASSERT_EQ(OB_SUCCESS, posting_value_reader.get_nested_key(1, persisted_nested_key_list));
  ASSERT_TRUE(persisted_nested_key_list.is_null());
  ASSERT_EQ(OB_SUCCESS,
      micro_block_reader.get_payload(1, posting_value_block, posting_value_block_size));
  ASSERT_EQ(OB_SUCCESS, posting_value_reader.init(posting_value_block, posting_value_block_size));
  ASSERT_EQ(OB_SUCCESS, posting_value_reader.get_nested_key(0, persisted_nested_key_list));
  ASSERT_EQ(expected_nested_key_list, persisted_nested_key_list.get_string());
  adapter.reset();
}

// 10.4 search_index_no_bm25_frontier: search index has no relevance params,
// so logical block metas carry no BM25 frontier pairs.
TEST_F(ObInvIdxMacroBlockWriterTest, search_index_no_bm25_frontier)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, prepare_search_open_env());
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObDatumRow row;
  for (int64_t i = 1; i <= 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, make_search_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
        "1", "$.a", "val_a", static_cast<uint64_t>(i), "nk_a"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, make_search_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "2", "$.b", "val_b", 6, "nk_b"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  EXPECT_TRUE(writer.micro_term_groups_.empty());
  const ObInvIdxLogicalBlockGroup &group = writer.pending_term_group_;
  ASSERT_EQ(1, group.logical_block_metas_.count());
  const ObInvIdxLogicalBlockMeta &meta = group.logical_block_metas_.at(0);
  EXPECT_EQ(5, meta.doc_count_);
  EXPECT_EQ(0, meta.frontier_pairs_.count());
  EXPECT_EQ(0ul, meta.total_doc_length_);
  EXPECT_EQ(5, writer.current_micro_stats_.doc_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_EQ(1, writer.posting_encoder_.get_doc_count());

  ASSERT_EQ(OB_SUCCESS, writer.close());
  writer.reset();
}

// 10.5 search_index_check_row_deleted: check_row_deleted uses inv_idx_row_desc_.doc_id_col_idx_
// instead of the old hardcoded column index 1. With a search index (3-col term), doc_id is at
// index 3; the old code would read the PATH column (index 1, a string) as doc_id, producing
// wrong results. This case verifies the fix reads the correct column and the bitmap lookup
// hits/misses on the right doc_id.
TEST_F(ObInvIdxMacroBlockWriterTest, search_index_check_row_deleted)
{
  ASSERT_EQ(OB_SUCCESS, prepare_search_open_env());
  ASSERT_EQ(INV_IDX_TYPE_SEARCH, inv_idx_row_desc_.index_type_);
  // doc_id must NOT be at index 1 (the old hardcoded position) for this test to be meaningful
  ASSERT_GE(inv_idx_row_desc_.doc_id_col_idx_, 2);

  // Build a deletion bitmap containing doc_id = 5
  common::ObRoaringBitmap *bitmap = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bitmap);
  ASSERT_EQ(OB_SUCCESS, bitmap->value_add(5));

  // Wrap it in a handle (init_from_bitmap is private; #define private public exposes it).
  // The handle's destructor will free the bitmap via allocator_.
  storage::ObDeletionBitmapHandle del_handle;
  ASSERT_EQ(OB_SUCCESS, del_handle.init_from_bitmap(allocator_, bitmap));

  // Dummy read_info — check_row_deleted does not use read_info_
  ObReadInfoStruct read_info;

  // Construct the merge helper with the search index row desc
  ObInvSSTableMergeHelper helper(read_info, inv_idx_row_desc_, allocator_, del_handle);

  // Row with doc_id=5 at doc_id_col_idx_ (index 3); index 1 is a string (PATH), not doc_id
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, make_search_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "1", "$.a", "val_a", 5 /*doc_id*/, "nk_a"));
  bool deleted = false;
  ASSERT_EQ(OB_SUCCESS, helper.check_row_deleted(row, deleted));
  EXPECT_TRUE(deleted);  // doc_id=5 is in the bitmap

  // doc_id=6 is NOT in the bitmap
  ASSERT_EQ(OB_SUCCESS, make_search_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "1", "$.a", "val_a", 6 /*doc_id*/, "nk_a"));
  ASSERT_EQ(OB_SUCCESS, helper.check_row_deleted(row, deleted));
  EXPECT_FALSE(deleted);
}

// =============================================================================
// Group 11 — 整宏块复用（append_macro_block）
// =============================================================================
//
// 测试目录：
//  11.1 append_row_after_reuse_restarts_macro_row_offset
//       - fresh → reuse → fresh 顺序；复用后 macro row offset 从 -1 重新开始；
//       - 逆序 term 拒绝；复用宏保留在最终 data-block 列表中间。
//  11.2 append_macro_block_requires_open_writer
//       - 未 open 的 writer 调用 append_macro_block 返回 OB_NOT_INIT。
//  11.3 append_macro_block_validates_descriptor
//       - null meta、clustered 不匹配等非法 descriptor 校验。
//  11.4 append_macro_block_rejects_invalid_stats_before_mutation
//       - 非法复用统计在修改 writer 状态前被拒绝。
//  11.5 append_macro_block_clustered_requires_micro_block_data
//       - clustered 环境的 writer 复用时 micro_block_data 为空被拒绝，writer 状态不变。
//  11.6 append_macro_block_clustered_rejects_nonclustered_desc
//       - clustered 环境的 writer 拒绝非 clustered descriptor（反向树类型不匹配）。
//  11.7 sstable_index_builder_reset_clears_async_index_flag
//       - builder reset 清除 async-index 标记。
//
// A reused macro is already complete. Its doc count must not become the base row offset of the
// next freshly built macro; offsets are local to each macro and start at -1. Cover the production
// order fresh -> reused -> fresh, then close the shared index builder and verify the result keeps
// the reused macro in the middle of the final data-block list.
TEST_F(ObInvIdxMacroBlockWriterTest, append_row_after_reuse_restarts_macro_row_offset)
{
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());

  ObSSTableIndexBuilder donor_builder(false /*use_double_write_buffer*/);
  ObInvIdxMacroBlockWriter donor_writer;
  ObSSTablePrivateObjectCleaner donor_cleaner;
  ObDataMacroBlockMeta *reused = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_reusable_macro_meta(
      donor_builder, donor_writer, donor_cleaner, allocator_, reused));
  ASSERT_NE(nullptr, reused);
  ASSERT_EQ(20, reused->val_.row_count_);

  ObSSTableIndexBuilder reuser_builder(false /*use_double_write_buffer*/);
  ASSERT_EQ(OB_SUCCESS, reuser_builder.init(data_store_desc_));
  data_store_desc_.sstable_index_builder_ = &reuser_builder;

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  // Write and finish a normal macro before the reused block. This makes the test cover the same
  // mixed sequence produced by compaction when only the middle range is eligible for reuse.
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "a", 1, 1, 100, "p"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  ObMacroBlockDesc desc;
  make_reuse_desc(*reused, desc);
  ObInvMacroMeta stats;
  ObInvIdxStatsMeta stats_value;
  ASSERT_EQ(OB_SUCCESS, stats_value.assign(writer.get_seg_inv_meta().get_stats()));
  stats_value.min_doc_id_ = 1;
  stats_value.max_doc_id_ = 20;
  stats_value.total_doc_length_ = 2000;
  ASSERT_EQ(OB_SUCCESS, stats.init(stats_value));
  ASSERT_EQ(OB_SUCCESS, writer.append_macro_block(desc, stats));
  EXPECT_EQ(-1, writer.index_block_builder_.row_offset_);
  EXPECT_EQ(1, writer.get_merge_block_info().multiplexed_macro_block_count_);
  EXPECT_EQ(2, writer.get_merge_block_info().macro_block_count_);
  EXPECT_EQ(1 + reused->val_.row_count_, writer.get_merge_block_info().total_row_count_);
  EXPECT_EQ(0, writer.macro_stats_.doc_count_);
  EXPECT_TRUE(writer.macro_stats_.endkey_.is_valid());
  EXPECT_EQ(0, writer.index_block_builder_.actual_index_micro_writer_->get_row_count());
  EXPECT_EQ(0, writer.index_block_builder_.trial_index_micro_writer_->get_row_count());
  EXPECT_EQ(-1, writer.index_block_builder_.row_offset_);

  // append_macro_block records the reused endkey. A later term that moves backwards must be
  // rejected even though no freshly-built macro is currently open.
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "b", 21, 1, 100, "p"));
  EXPECT_EQ(OB_ERR_UNEXPECTED, writer.append_row(row));

  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "z", 21, 1, 100, "p"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  ASSERT_EQ(OB_SUCCESS, writer.flush_logical_block());
  ASSERT_EQ(OB_SUCCESS, writer.flush_micro_block_pair());
  EXPECT_EQ(1, writer.macro_stats_.doc_count_);
  EXPECT_EQ(0, writer.index_block_builder_.row_offset_);
  EXPECT_EQ(1, writer.index_block_builder_.actual_index_micro_writer_->get_row_count());
  EXPECT_EQ(1, writer.index_block_builder_.trial_index_micro_writer_->get_row_count());
  EXPECT_EQ(2, writer.macro_block_.get_micro_block_count());

  ASSERT_EQ(OB_SUCCESS, writer.close());
  ObSSTableMergeRes res;
  ASSERT_EQ(OB_SUCCESS, reuser_builder.close(res));
  ASSERT_EQ(22, res.row_count_);
  ASSERT_EQ(3, res.data_block_ids_.count());
  ASSERT_EQ(1, res.use_old_macro_block_count_);
  EXPECT_NE(reused->get_macro_id(), res.data_block_ids_.at(0));
  EXPECT_EQ(reused->get_macro_id(), res.data_block_ids_.at(1));
  EXPECT_NE(reused->get_macro_id(), res.data_block_ids_.at(2));
  EXPECT_EQ(1ul, writer.get_seg_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(21ul, writer.get_seg_inv_meta().get_stats().max_doc_id_);
  EXPECT_EQ(2002ul, writer.get_seg_inv_meta().get_stats().total_doc_length_);
}

TEST_F(ObInvIdxMacroBlockWriterTest, append_macro_block_requires_open_writer)
{
  ObInvIdxMacroBlockWriter writer;
  ObMacroBlockDesc desc;
  ObInvMacroMeta stats;
  ASSERT_EQ(OB_NOT_INIT, writer.append_macro_block(desc, stats));
  EXPECT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);
  EXPECT_EQ(0, writer.get_merge_block_info().macro_block_count_);
}

TEST_F(ObInvIdxMacroBlockWriterTest, append_macro_block_validates_descriptor)
{
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObMacroBlockDesc desc;
  ObInvMacroMeta stats;
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.append_macro_block(desc, stats));
  EXPECT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);

  ObSSTableIndexBuilder donor_builder(false /*use_double_write_buffer*/);
  ObInvIdxMacroBlockWriter donor_writer;
  ObSSTablePrivateObjectCleaner donor_cleaner;
  ObDataMacroBlockMeta *reused = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_reusable_macro_meta(
      donor_builder, donor_writer, donor_cleaner, allocator_, reused));
  ASSERT_NE(nullptr, reused);
  make_reuse_desc(*reused, desc);
  desc.is_clustered_index_tree_ = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, writer.append_macro_block(desc, stats));
  EXPECT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);
  EXPECT_EQ(0, writer.get_merge_block_info().macro_block_count_);
}

TEST_F(ObInvIdxMacroBlockWriterTest, append_macro_block_rejects_invalid_stats_before_mutation)
{
  ASSERT_EQ(OB_SUCCESS, prepare_open_env());
  ObSSTableIndexBuilder donor_builder(false /*use_double_write_buffer*/);
  ObInvIdxMacroBlockWriter donor_writer;
  ObSSTablePrivateObjectCleaner donor_cleaner;
  ObDataMacroBlockMeta *reused = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_reusable_macro_meta(
      donor_builder, donor_writer, donor_cleaner, allocator_, reused));
  ASSERT_NE(nullptr, reused);

  ObSSTableIndexBuilder reuser_builder(false /*use_double_write_buffer*/);
  ASSERT_EQ(OB_SUCCESS, reuser_builder.init(data_store_desc_));
  data_store_desc_.sstable_index_builder_ = &reuser_builder;

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
      "a", 1, 1, 100, "p"));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  ASSERT_TRUE(writer.has_pending_rows_);
  const int64_t row_offset = writer.index_block_builder_.row_offset_;
  const auto expect_unchanged = [&]() {
    EXPECT_TRUE(writer.has_pending_rows_);
    EXPECT_EQ(1, writer.posting_encoder_.get_doc_count());
    EXPECT_EQ(row_offset, writer.index_block_builder_.row_offset_);
    EXPECT_EQ(0, writer.get_macro_block_write_ctx().get_macro_block_count());
    EXPECT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);
    EXPECT_EQ(0, writer.get_merge_block_info().macro_block_count_);
    EXPECT_EQ(0, writer.get_merge_block_info().total_row_count_);
    EXPECT_TRUE(writer.get_seg_inv_meta().get_stats().is_empty_range());
    EXPECT_EQ(0, writer.get_seg_inv_meta().get_stats().total_doc_length_);
  };
  ObMacroBlockDesc desc;
  make_reuse_desc(*reused, desc);
  ObInvMacroMeta stats;
  ObInvIdxStatsMeta invalid_stats;
  invalid_stats.min_doc_id_ = 2;
  invalid_stats.max_doc_id_ = 1;
  // Inject malformed metadata to exercise the writer validation.
  ASSERT_EQ(OB_SUCCESS, stats.stats_.assign(invalid_stats));
  stats.is_inited_ = true;

  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_macro_block(desc, stats));
  expect_unchanged();

  // Default macro metadata has neither a populated range nor a valid layout.
  stats.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_macro_block(desc, stats));
  expect_unchanged();

  ObInvIdxStatsMeta incompatible;
  ASSERT_EQ(OB_SUCCESS, incompatible.assign(writer.get_seg_inv_meta().get_stats()));
  incompatible.min_doc_id_ = 1;
  incompatible.max_doc_id_ = 20;
  incompatible.has_position_list_ = !incompatible.has_position_list_;
  ASSERT_TRUE(incompatible.is_valid());
  ASSERT_EQ(OB_SUCCESS, stats.init(incompatible));
  ASSERT_EQ(OB_ERR_UNEXPECTED, writer.append_macro_block(desc, stats));
  expect_unchanged();
}

// 11.5 append_macro_block_clustered_requires_micro_block_data
//       - clustered 环境的 writer 复用 clustered 宏块时 micro_block_data 为空被拒绝，
//         writer 状态不变。
TEST_F(ObInvIdxMacroBlockWriterTest, append_macro_block_clustered_requires_micro_block_data)
{
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(true /*has_pos_list*/, true /*has_relevance*/,
      "none", compaction::MINI_MERGE, true /*micro_index_clustered*/));

  ObSSTableIndexBuilder donor_builder(false /*use_double_write_buffer*/);
  ObInvIdxMacroBlockWriter donor_writer;
  ObSSTablePrivateObjectCleaner donor_cleaner;
  ObDataMacroBlockMeta *reused = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_reusable_macro_meta(
      donor_builder, donor_writer, donor_cleaner, allocator_, reused));
  ASSERT_NE(nullptr, reused);

  ObSSTableIndexBuilder reuser_builder(false /*use_double_write_buffer*/);
  ASSERT_EQ(OB_SUCCESS, reuser_builder.init(data_store_desc_));
  data_store_desc_.sstable_index_builder_ = &reuser_builder;

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObMacroBlockDesc desc;
  make_reuse_desc(*reused, desc);
  desc.is_clustered_index_tree_ = true;
  ObInvMacroMeta stats;
  ObInvIdxStatsMeta stats_value;
  ASSERT_EQ(OB_SUCCESS, stats_value.assign(writer.get_seg_inv_meta().get_stats()));
  stats_value.min_doc_id_ = 1;
  stats_value.max_doc_id_ = 20;
  stats_value.total_doc_length_ = 2000;
  ASSERT_EQ(OB_SUCCESS, stats.init(stats_value));
  // The clustered replay path requires the reused macro's clustered index micro block;
  // a null payload must be rejected before any writer state changes.
  EXPECT_EQ(OB_ERR_UNEXPECTED, writer.append_macro_block(desc, stats, nullptr));
  EXPECT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);
  EXPECT_EQ(0, writer.get_merge_block_info().macro_block_count_);
  EXPECT_EQ(0, writer.get_merge_block_info().total_row_count_);
}

// 11.6 append_macro_block_clustered_rejects_nonclustered_desc
//       - clustered 环境的 writer 拒绝 is_clustered_index_tree_=false 的 descriptor
//         （与 11.3 反向的树类型不匹配）。
TEST_F(ObInvIdxMacroBlockWriterTest, append_macro_block_clustered_rejects_nonclustered_desc)
{
  ASSERT_EQ(OB_SUCCESS, prepare_open_env(true /*has_pos_list*/, true /*has_relevance*/,
      "none", compaction::MINI_MERGE, true /*micro_index_clustered*/));

  ObSSTableIndexBuilder donor_builder(false /*use_double_write_buffer*/);
  ObInvIdxMacroBlockWriter donor_writer;
  ObSSTablePrivateObjectCleaner donor_cleaner;
  ObDataMacroBlockMeta *reused = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_reusable_macro_meta(
      donor_builder, donor_writer, donor_cleaner, allocator_, reused));
  ASSERT_NE(nullptr, reused);

  ObSSTableIndexBuilder reuser_builder(false /*use_double_write_buffer*/);
  ASSERT_EQ(OB_SUCCESS, reuser_builder.init(data_store_desc_));
  data_store_desc_.sstable_index_builder_ = &reuser_builder;

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, cleaner));

  ObMacroBlockDesc desc;
  make_reuse_desc(*reused, desc);   // is_clustered_index_tree_ = false
  ObInvMacroMeta stats;
  EXPECT_EQ(OB_ERR_UNEXPECTED, writer.append_macro_block(desc, stats));
  EXPECT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);
  EXPECT_EQ(0, writer.get_merge_block_info().macro_block_count_);
}

TEST_F(ObInvIdxMacroBlockWriterTest, merge_inv_meta_concurrent_partials)
{
  ObSSTableIndexBuilder builder(false /*use_double_write_buffer*/);
  builder.set_is_async_index(true);
  const int64_t thread_count = 8;
  const int64_t iterations = 1024;
  std::atomic<int64_t> ready(0);
  std::atomic<bool> start(false);
  std::thread threads[thread_count];
  int results[thread_count] = {};
  for (int64_t i = 0; i < thread_count; ++i) {
    threads[i] = std::thread([&, i]() {
      const ObInvSSTableMeta partial = make_inv_meta(thread_count - i, 100 + i, i + 1);
      ready.fetch_add(1);
      while (!start.load()) {
        std::this_thread::yield();
      }
      for (int64_t j = 0; OB_SUCCESS == results[i] && j < iterations; ++j) {
        results[i] = builder.merge_inv_meta(partial);
      }
    });
  }
  while (ready.load() != thread_count) {
    std::this_thread::yield();
  }
  start.store(true);
  for (int64_t i = 0; i < thread_count; ++i) {
    threads[i].join();
    EXPECT_EQ(OB_SUCCESS, results[i]);
  }
  const ObInvIdxStatsMeta &meta = builder.get_inv_meta().get_stats();
  EXPECT_TRUE(builder.get_inv_meta().is_valid());
  EXPECT_EQ(1U, meta.min_doc_id_);
  EXPECT_EQ(107U, meta.max_doc_id_);
  EXPECT_EQ(static_cast<uint64_t>(iterations * thread_count * (thread_count + 1) / 2),
      meta.total_doc_length_);
}

TEST_F(ObInvIdxMacroBlockWriterTest, merge_inv_meta_accepts_empty_partials)
{
  ObSSTableIndexBuilder builder(false /*use_double_write_buffer*/);
  builder.set_is_async_index(true);
  const ObInvSSTableMeta empty = make_inv_meta();
  ASSERT_EQ(OB_SUCCESS, builder.merge_inv_meta(empty));
  ASSERT_EQ(OB_SUCCESS, builder.merge_inv_meta(empty));
  EXPECT_TRUE(builder.get_inv_meta().is_valid());
  EXPECT_TRUE(builder.get_inv_meta().get_stats().is_empty_range());

  const ObInvSSTableMeta partial = make_inv_meta(3, 9, 7);
  ASSERT_EQ(OB_SUCCESS, builder.merge_inv_meta(partial));
  ASSERT_EQ(OB_SUCCESS, builder.merge_inv_meta(empty));
  EXPECT_EQ(3U, builder.get_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(9U, builder.get_inv_meta().get_stats().max_doc_id_);
  EXPECT_EQ(7U, builder.get_inv_meta().get_stats().total_doc_length_);
}

TEST_F(ObInvIdxMacroBlockWriterTest, merge_inv_meta_rejects_invalid_and_inconsistent_partials)
{
  ObSSTableIndexBuilder builder(false /*use_double_write_buffer*/);
  builder.set_is_async_index(true);
  const ObInvSSTableMeta invalid;
  EXPECT_EQ(OB_INVALID_ARGUMENT, builder.merge_inv_meta(invalid));
  EXPECT_FALSE(builder.get_inv_meta().is_valid());

  const ObInvSSTableMeta partial = make_inv_meta(3, 9, 7);
  ASSERT_EQ(OB_SUCCESS, builder.merge_inv_meta(partial));
  ObInvIdxStatsMeta incompatible_stats = make_inv_meta().get_stats();
  incompatible_stats.has_position_list_ = false;
  ObInvSSTableMeta incompatible_empty;
  ASSERT_EQ(OB_SUCCESS, incompatible_empty.init(incompatible_stats));
  EXPECT_EQ(OB_ERR_UNEXPECTED, builder.merge_inv_meta(incompatible_empty));
  EXPECT_EQ(3U, builder.get_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(9U, builder.get_inv_meta().get_stats().max_doc_id_);
  EXPECT_EQ(7U, builder.get_inv_meta().get_stats().total_doc_length_);
  EXPECT_TRUE(builder.get_inv_meta().get_stats().has_position_list_);
}

TEST_F(ObInvIdxMacroBlockWriterTest, merge_inv_meta_saturates_total_doc_length)
{
  ObSSTableIndexBuilder builder(false /*use_double_write_buffer*/);
  builder.set_is_async_index(true);
  ASSERT_EQ(OB_SUCCESS, builder.merge_inv_meta(make_inv_meta(3, 7, UINT64_MAX - 1)));
  ASSERT_EQ(OB_SUCCESS, builder.merge_inv_meta(make_inv_meta(2, 8, 2)));
  EXPECT_EQ(2U, builder.get_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(8U, builder.get_inv_meta().get_stats().max_doc_id_);
  EXPECT_EQ(UINT64_MAX, builder.get_inv_meta().get_stats().total_doc_length_);
}

TEST_F(ObInvIdxMacroBlockWriterTest, sstable_index_builder_reset_clears_async_index_flag)
{
  ObSSTableIndexBuilder builder(false /*use_double_write_buffer*/);
  builder.set_is_async_index(true);
  ASSERT_TRUE(builder.is_async_index_);

  builder.reset();
  ASSERT_FALSE(builder.is_async_index_);
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 1;
  system("rm -f test_inv_idx_macro_block_writer.log*");
  // OB_LOGGER.set_file_name("test_inv_idx_macro_block_writer.log");
  // OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  oceanbase::plugin::ObPluginMgr plugins;
  if (plugins.init(oceanbase::common::ObString()) == oceanbase::common::OB_SUCCESS
      && plugins.load_builtin_plugins() == oceanbase::common::OB_SUCCESS) {
    GCTX.plugin_mgr_ = &plugins;
    ret = RUN_ALL_TESTS();
    GCTX.plugin_mgr_ = nullptr;
  }
  return ret;
}

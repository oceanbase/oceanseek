/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/search_index/ob_search_index_config.h"
#include "unittest/share/search_index/search_index_config_test_util.h"
#define private public
#include "storage/ddl/ob_async_index_macro_writer.h"
#undef private

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace share;

class TestAsyncIndexMacroWriter : public ::testing::Test
{
public:
  static int64_t nested_key_storage_idx()
  {
    return SEARCH_INDEX_KEY_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  }

  static int64_t storage_column_count()
  {
    return nested_key_storage_idx() + 1;
  }

  static void init_nested_row_desc(ObInvIdxRowDesc &row_desc)
  {
    row_desc.reset();
    row_desc.doc_id_col_idx_ = SEARCH_INDEX_DOC_ID;
    row_desc.nested_key_col_idx_ = static_cast<int16_t>(nested_key_storage_idx());
    row_desc.index_type_ = INV_IDX_TYPE_SEARCH;
  }

  static int build_row(
      ObDatumRow &row,
      const uint64_t doc_id,
      const ObString &nested_key,
      const bool is_null,
      const bool with_partition_key = false)
  {
    int ret = OB_SUCCESS;
    const int64_t column_count = storage_column_count() + (with_partition_key ? 1 : 0);
    if (!row.is_valid() && OB_FAIL(row.init(column_count))) {
    } else {
      row.count_ = column_count;
      row.storage_datums_[0].set_uint(1);
      row.storage_datums_[1].set_string(ObString::make_string("path"));
      row.storage_datums_[2].set_string(ObString::make_string("value"));
      row.storage_datums_[SEARCH_INDEX_DOC_ID].set_uint(doc_id);
      row.storage_datums_[SEARCH_INDEX_KEY_CNT].set_int(-1000);
      row.storage_datums_[SEARCH_INDEX_KEY_CNT + 1].set_int(0);
      if (is_null) {
        row.storage_datums_[nested_key_storage_idx()].set_null();
      } else {
        row.storage_datums_[nested_key_storage_idx()].set_string(nested_key);
      }
      if (with_partition_key) {
        row.storage_datums_[storage_column_count()].set_int(42);
      }
    }
    return ret;
  }

  static int init_reader(
      const char *buf,
      const int64_t size,
      ObSearchIndexNestedKeyListReader &reader)
  {
    ObDatum datum;
    datum.set_string(ObString(static_cast<int32_t>(size), buf));
    return reader.init(datum);
  }
};

TEST_F(TestAsyncIndexMacroWriter, shared_length_format_golden_round_trip)
{
  const char expected_list[] = {1, 0, 0, 0, 2, 1, 'a', 'b'};
  ObArenaAllocator allocator;
  ObStringBuffer buffer(&allocator);
  ObSearchIndexNestedKeyListWriter writer;
  ObSearchIndexNestedKeyListReader reader;
  ObStorageDatum key;
  ObDatum encoded;
  ObDatum decoded;
  ObString list;

  ASSERT_EQ(OB_SUCCESS, writer.init(buffer));
  key.set_string(ObString::make_string("a"));
  ASSERT_EQ(OB_SUCCESS, writer.append(key));
  key.set_string(ObString::make_string("b"));
  ASSERT_EQ(OB_SUCCESS, writer.append(key));
  ASSERT_EQ(OB_SUCCESS, writer.build(list));
  ASSERT_EQ(sizeof(expected_list), list.length());
  ASSERT_EQ(0, MEMCMP(expected_list, list.ptr(), sizeof(expected_list)));

  encoded.set_string(ObString(static_cast<int32_t>(sizeof(expected_list)), expected_list));
  ASSERT_EQ(OB_SUCCESS, reader.init(encoded));
  ASSERT_EQ(2U, reader.get_count());
  ASSERT_EQ(OB_SUCCESS, reader.first(decoded));
  ASSERT_EQ(ObString::make_string("a"), decoded.get_string());
  ASSERT_EQ(OB_SUCCESS, reader.next(decoded));
  ASSERT_EQ(ObString::make_string("b"), decoded.get_string());
  ASSERT_EQ(OB_ITER_END, reader.next(decoded));
}

TEST_F(TestAsyncIndexMacroWriter, reject_null_and_zero_length_nested_keys_in_codec)
{
  ObArenaAllocator allocator;
  ObStringBuffer buffer(&allocator);
  ObSearchIndexNestedKeyListWriter writer;
  ObStorageDatum key;

  ASSERT_EQ(OB_SUCCESS, writer.init(buffer));
  key.set_null();
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append(key));
  key.set_string(ObString());
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append(key));
  ASSERT_EQ(0U, writer.get_count());
  ASSERT_EQ(ObSearchIndexNestedKeyListFormat::HEADER_SIZE, buffer.length());
}

TEST_F(TestAsyncIndexMacroWriter, reject_different_nested_key_lengths_in_one_list)
{
  ObArenaAllocator allocator;
  ObStringBuffer buffer(&allocator);
  ObSearchIndexNestedKeyListWriter writer;
  ObStorageDatum key;

  ASSERT_EQ(OB_SUCCESS, writer.init(buffer));
  key.set_string(ObString::make_string("a"));
  ASSERT_EQ(OB_SUCCESS, writer.append(key));
  key.set_string(ObString::make_string("bc"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append(key));
  ASSERT_EQ(1U, writer.get_count());
  ASSERT_EQ(ObSearchIndexNestedKeyListFormat::HEADER_SIZE + 1, buffer.length());
}

TEST_F(TestAsyncIndexMacroWriter, reject_unordered_and_duplicate_nested_keys)
{
  const char expected_list[] = {1, 0, 0, 0, 2, 1, 'b', 'c'};
  ObArenaAllocator allocator;
  ObStringBuffer buffer(&allocator);
  ObSearchIndexNestedKeyListWriter writer;
  ObStorageDatum key;
  ObString list;

  ASSERT_EQ(OB_SUCCESS, writer.init(buffer));
  key.set_string(ObString::make_string("b"));
  ASSERT_EQ(OB_SUCCESS, writer.append(key));
  key.set_string(ObString::make_string("a"));
  ASSERT_EQ(OB_ROWKEY_ORDER_ERROR, writer.append(key));
  key.set_string(ObString::make_string("b"));
  ASSERT_EQ(OB_ROWKEY_ORDER_ERROR, writer.append(key));
  ASSERT_EQ(1U, writer.get_count());
  ASSERT_EQ(ObSearchIndexNestedKeyListFormat::HEADER_SIZE + 1, buffer.length());

  key.set_string(ObString::make_string("c"));
  ASSERT_EQ(OB_SUCCESS, writer.append(key));
  ASSERT_EQ(2U, writer.get_count());
  ASSERT_EQ(OB_SUCCESS, writer.build(list));
  ASSERT_EQ(sizeof(expected_list), list.length());
  ASSERT_EQ(0, MEMCMP(expected_list, list.ptr(), sizeof(expected_list)));
}

TEST_F(TestAsyncIndexMacroWriter, async_search_layout_keeps_nested_list_as_posting_value)
{
  ObInvIdxRowDesc row_desc;
  const int64_t nested_storage_column_cnt = SEARCH_INDEX_KEY_CNT
      + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t plain_storage_column_cnt = nested_storage_column_cnt;

  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMacroWriterAdapter::build_search_inv_idx_row_desc(
      true, SEARCH_INDEX_KEY_CNT, nested_storage_column_cnt + 1, row_desc));
  ASSERT_EQ(SEARCH_INDEX_DOC_ID, row_desc.doc_id_col_idx_);
  ASSERT_EQ(nested_storage_column_cnt, row_desc.nested_key_col_idx_);
  ASSERT_EQ(INV_IDX_TYPE_SEARCH, row_desc.index_type_);

  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObAsyncIndexMacroWriterAdapter::build_search_inv_idx_row_desc(
          true, SEARCH_INDEX_KEY_CNT_WITH_NK, nested_storage_column_cnt + 1, row_desc));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObAsyncIndexMacroWriterAdapter::build_search_inv_idx_row_desc(
          true, SEARCH_INDEX_KEY_CNT, nested_storage_column_cnt, row_desc));
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMacroWriterAdapter::build_search_inv_idx_row_desc(
      false, SEARCH_INDEX_KEY_CNT, plain_storage_column_cnt, row_desc));
  ASSERT_EQ(-1, row_desc.nested_key_col_idx_);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObAsyncIndexMacroWriterAdapter::build_search_inv_idx_row_desc(
          false, SEARCH_INDEX_KEY_CNT_WITH_NK, nested_storage_column_cnt, row_desc));
}

TEST_F(TestAsyncIndexMacroWriter, fold_same_posting_into_nested_key_list)
{
  ObAsyncIndexMacroWriterAdapter adapter;
  ObDatumRow row;
  init_nested_row_desc(adapter.inv_idx_row_desc_);
  adapter.is_inited_ = true;

  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("a"), false));
  ASSERT_EQ(OB_SUCCESS, adapter.append_row(row));
  ASSERT_EQ(INV_IDX_TYPE_SEARCH, adapter.inv_idx_row_desc_.index_type_);
  ASSERT_EQ(-1000, adapter.pending_nested_row_.storage_datums_[SEARCH_INDEX_KEY_CNT].get_int());
  ASSERT_EQ(0, adapter.pending_nested_row_.storage_datums_[SEARCH_INDEX_KEY_CNT + 1].get_int());
  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("b"), false));
  ASSERT_TRUE(adapter.is_same_posting(row));
  ASSERT_EQ(OB_SUCCESS, adapter.append_row(row));

  ASSERT_EQ(2, adapter.get_row_count());
  ASSERT_EQ(2U, adapter.nested_key_list_writer_.get_count());
  ASSERT_EQ(8U, adapter.nested_key_list_buffer_.length());
  ObString list;
  ObDatum list_datum;
  ObDatum nested_key;
  ObSearchIndexNestedKeyListReader reader;
  ASSERT_EQ(OB_SUCCESS, adapter.nested_key_list_writer_.build(list));
  list_datum.set_string(list);
  ASSERT_EQ(OB_SUCCESS, reader.init(list_datum));
  ASSERT_EQ(2U, reader.get_count());
  ASSERT_EQ(OB_SUCCESS, reader.first(nested_key));
  ASSERT_EQ(ObString::make_string("a"), nested_key.get_string());
  ASSERT_EQ(OB_SUCCESS, reader.next(nested_key));
  ASSERT_EQ(ObString::make_string("b"), nested_key.get_string());
  ASSERT_EQ(OB_ITER_END, reader.next(nested_key));
}

TEST_F(TestAsyncIndexMacroWriter, non_nested_row_uses_direct_writer_path)
{
  ObAsyncIndexMacroWriterAdapter adapter;
  ObDatumRow row;
  adapter.is_inited_ = true;
  adapter.inv_idx_row_desc_.reset();
  const int64_t storage_col_cnt = SEARCH_INDEX_KEY_CNT
      + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ASSERT_EQ(OB_SUCCESS, row.init(storage_col_cnt));
  row.count_ = storage_col_cnt;
  row.storage_datums_[0].set_uint(1);
  row.storage_datums_[1].set_string(ObString::make_string("path"));
  row.storage_datums_[2].set_string(ObString::make_string("value"));
  row.storage_datums_[SEARCH_INDEX_DOC_ID].set_uint(100);
  row.storage_datums_[SEARCH_INDEX_KEY_CNT].set_int(-1000);
  row.storage_datums_[SEARCH_INDEX_KEY_CNT + 1].set_int(0);

  // The underlying writer is deliberately unopened. Reaching its OB_NOT_INIT
  // proves the non-nested public path is a direct pass-through, not a fold.
  ASSERT_EQ(OB_NOT_INIT, adapter.append_row(row));
  ASSERT_EQ(0, adapter.get_row_count());
  ASSERT_FALSE(adapter.has_pending_nested_group_);
}

TEST_F(TestAsyncIndexMacroWriter, posting_key_includes_all_terms_and_doc_id)
{
  ObAsyncIndexMacroWriterAdapter adapter;
  ObDatumRow row;
  init_nested_row_desc(adapter.inv_idx_row_desc_);

  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("a"), false));
  ASSERT_EQ(OB_SUCCESS, adapter.append_nested_search_row(row));

  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("b"), false));
  row.storage_datums_[0].set_uint(2);
  ASSERT_FALSE(adapter.is_same_posting(row));

  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("b"), false));
  row.storage_datums_[1].set_string(ObString::make_string("other_path"));
  ASSERT_FALSE(adapter.is_same_posting(row));

  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("b"), false));
  row.storage_datums_[2].set_string(ObString::make_string("other_value"));
  ASSERT_FALSE(adapter.is_same_posting(row));

  ASSERT_EQ(OB_SUCCESS, build_row(row, 101, ObString::make_string("b"), false));
  ASSERT_FALSE(adapter.is_same_posting(row));
}

TEST_F(TestAsyncIndexMacroWriter, preserve_mvcc_and_partition_tail_in_pending_row)
{
  ObAsyncIndexMacroWriterAdapter adapter;
  ObDatumRow row;
  init_nested_row_desc(adapter.inv_idx_row_desc_);

  ASSERT_EQ(OB_SUCCESS, build_row(
      row, 100, ObString::make_string("a"), false, true));
  ASSERT_EQ(OB_SUCCESS, adapter.append_nested_search_row(row));
  ASSERT_EQ(row.get_column_count(), adapter.pending_nested_row_.get_column_count());
  ASSERT_EQ(-1000,
      adapter.pending_nested_row_.storage_datums_[SEARCH_INDEX_KEY_CNT].get_int());
  ASSERT_EQ(0,
      adapter.pending_nested_row_.storage_datums_[SEARCH_INDEX_KEY_CNT + 1].get_int());
  ASSERT_EQ(42,
      adapter.pending_nested_row_.storage_datums_[storage_column_count()].get_int());
}

TEST_F(TestAsyncIndexMacroWriter, reject_mixed_null_and_non_null_nested_keys)
{
  ObAsyncIndexMacroWriterAdapter adapter;
  ObDatumRow row;
  init_nested_row_desc(adapter.inv_idx_row_desc_);

  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString(), true));
  ASSERT_EQ(OB_SUCCESS, adapter.append_nested_search_row(row));
  ASSERT_EQ(OB_SUCCESS, adapter.append_nested_search_row(row));

  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("a"), false));
  ASSERT_EQ(OB_ERR_UNEXPECTED, adapter.append_nested_search_row(row));
}

TEST_F(TestAsyncIndexMacroWriter, propagate_nested_key_order_errors)
{
  ObAsyncIndexMacroWriterAdapter adapter;
  ObDatumRow row;
  init_nested_row_desc(adapter.inv_idx_row_desc_);

  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("b"), false));
  ASSERT_EQ(OB_SUCCESS, adapter.append_nested_search_row(row));
  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("a"), false));
  ASSERT_EQ(OB_ROWKEY_ORDER_ERROR, adapter.append_nested_search_row(row));
  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("b"), false));
  ASSERT_EQ(OB_ROWKEY_ORDER_ERROR, adapter.append_nested_search_row(row));
  ASSERT_EQ(OB_SUCCESS, build_row(row, 100, ObString::make_string("c"), false));
  ASSERT_EQ(OB_SUCCESS, adapter.append_nested_search_row(row));
}

TEST_F(TestAsyncIndexMacroWriter, reject_nested_key_list_over_serialized_size_limit)
{
  ObArenaAllocator allocator;
  ObStringBuffer buffer(&allocator);
  ObSearchIndexNestedKeyListWriter writer;
  ObStorageDatum key;
  const int64_t max_size = ObSearchIndexNestedKeyListFormat::HEADER_SIZE + 1;

  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.init(buffer, -1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.init(
      buffer, ObSearchIndexNestedKeyListFormat::HEADER_SIZE - 1));
  ASSERT_EQ(OB_SUCCESS, writer.init(buffer, max_size));
  key.set_string(ObString::make_string("a"));
  ASSERT_EQ(OB_SUCCESS, writer.append(key));
  ASSERT_EQ(1U, writer.get_count());
  ASSERT_EQ(max_size, buffer.length());

  key.set_string(ObString::make_string("b"));
  ASSERT_EQ(OB_SIZE_OVERFLOW, writer.append(key));
  ASSERT_EQ(1U, writer.get_count());
  ASSERT_EQ(max_size, buffer.length());
}

TEST_F(TestAsyncIndexMacroWriter, reject_malformed_nested_key_lists)
{
  const char bad_version[] = {2, 0, 0, 0, 1, 1, 'a'};
  const char zero_count[] = {1, 0, 0, 0, 0, 1};
  const char zero_length_key[] = {1, 0, 0, 0, 1, 0};
  const char oversized_key[] = {1, 0, 0, 0, 1, 81};
  const char truncated[] = {1, 0, 0, 0, 2, 1, 'a'};
  const char trailing[] = {1, 0, 0, 0, 1, 1, 'a', 'x'};
  ObSearchIndexNestedKeyListReader reader;

  ASSERT_EQ(OB_INVALID_ARGUMENT, init_reader(bad_version, sizeof(bad_version), reader));
  ASSERT_EQ(OB_INVALID_ARGUMENT, init_reader(zero_count, sizeof(zero_count), reader));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      init_reader(zero_length_key, sizeof(zero_length_key), reader));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      init_reader(oversized_key, sizeof(oversized_key), reader));
  ASSERT_EQ(OB_INVALID_ARGUMENT, init_reader(truncated, sizeof(truncated), reader));
  ASSERT_EQ(OB_INVALID_ARGUMENT, init_reader(trailing, sizeof(trailing), reader));
}

TEST_F(TestAsyncIndexMacroWriter, reader_relies_on_writer_for_nested_key_order)
{
  const char unordered[] = {1, 0, 0, 0, 2, 1, 'b', 'a'};
  const char duplicate[] = {1, 0, 0, 0, 2, 1, 'a', 'a'};
  ObSearchIndexNestedKeyListReader reader;
  ObDatum decoded;

  // The writer enforces key ordering. The reader only validates the header
  // and exact size, and sequential reads preserve the serialized key order.
  ASSERT_EQ(OB_SUCCESS, init_reader(unordered, sizeof(unordered), reader));
  ASSERT_EQ(2U, reader.get_count());
  ASSERT_EQ(OB_SUCCESS, reader.first(decoded));
  ASSERT_EQ(ObString::make_string("b"), decoded.get_string());
  ASSERT_EQ(OB_SUCCESS, reader.next(decoded));
  ASSERT_EQ(ObString::make_string("a"), decoded.get_string());
  ASSERT_EQ(OB_ITER_END, reader.next(decoded));

  ASSERT_EQ(OB_SUCCESS, init_reader(duplicate, sizeof(duplicate), reader));
  ASSERT_EQ(2U, reader.get_count());
  ASSERT_EQ(OB_SUCCESS, reader.first(decoded));
  ASSERT_EQ(ObString::make_string("a"), decoded.get_string());
  ASSERT_EQ(OB_SUCCESS, reader.next(decoded));
  ASSERT_EQ(ObString::make_string("a"), decoded.get_string());
  ASSERT_EQ(OB_ITER_END, reader.next(decoded));
}

// =============================================================================
// Search Text row desc: (COL_IDX, PATH, TOKEN, DOC_ID) rowkey, TF/DL[/POS] values.
// =============================================================================
struct SearchTextColMeta
{
  uint64_t column_id_;
  int64_t rowkey_pos_;
  ObObjType obj_type_;
  ObCollationType cs_type_;
  uint64_t flags_;
  bool nullable_;
};

class TestSearchTextRowDesc : public ::testing::Test
{
public:
  static const int64_t MAX_COL_CNT = 8;
  static const uint64_t TABLE_ID = 8888;

  static int64_t init_col_metas(const bool has_pos_list, SearchTextColMeta *metas)
  {
    int64_t cnt = 0;
    metas[cnt++] = {OB_APP_MIN_COLUMN_ID, 1, ObUInt64Type, CS_TYPE_BINARY, 0, false};
    metas[cnt++] = {OB_APP_MIN_COLUMN_ID + 1, 2, ObVarcharType, CS_TYPE_BINARY, 0, false};
    metas[cnt++] = {OB_APP_MIN_COLUMN_ID + 2, 3, ObVarcharType, CS_TYPE_BINARY, 0, false};
    metas[cnt++] = {OB_APP_MIN_COLUMN_ID + 3, 4, ObUInt64Type, CS_TYPE_BINARY, 0, false};
    metas[cnt++] = {OB_APP_MIN_COLUMN_ID + 4, 0, ObUInt64Type, CS_TYPE_BINARY,
        0, false};
    metas[cnt++] = {OB_APP_MIN_COLUMN_ID + 5, 0, ObUInt64Type, CS_TYPE_BINARY,
        0, false};
    if (has_pos_list) {
      metas[cnt++] = {OB_APP_MIN_COLUMN_ID + 6, 0, ObVarcharType, CS_TYPE_BINARY,
          0, false};
    }
    return cnt;
  }

  static int build_schema(
      const SearchTextColMeta *metas,
      const int64_t column_cnt,
      share::schema::ObTableSchema &table_schema,
      const share::schema::ObIndexType index_type =
          share::schema::INDEX_TYPE_MAX)
  {
    int ret = OB_SUCCESS;
    int64_t rowkey_cnt = 0;
    uint64_t max_column_id = 0;
    table_schema.reset();
    table_schema.set_tenant_id(1);
    table_schema.set_database_id(1);
    table_schema.set_table_id(TABLE_ID);
    table_schema.set_index_type(index_type == share::schema::INDEX_TYPE_MAX
        ? (column_cnt == 7 ? share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL
                           : share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL)
        : index_type);
    // index_type implies an index table; reset() leaves USER_TABLE which fails
    // ObSimpleTableSchemaV2::is_valid() (table_type/index_type mismatch, then data_table_id).
    table_schema.set_table_type(share::schema::USER_INDEX);
    table_schema.set_data_table_id(9999);
    for (int64_t i = 0; i < column_cnt; ++i) {
      rowkey_cnt += metas[i].rowkey_pos_ > 0 ? 1 : 0;
      max_column_id = MAX(max_column_id, metas[i].column_id_);
    }
    table_schema.set_rowkey_column_num(rowkey_cnt);
    table_schema.set_max_used_column_id(max_column_id);
    if (OB_FAIL(table_schema.set_table_name("test_search_text"))) {
      LOG_WARN("fail to set table name", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      share::schema::ObColumnSchemaV2 column;
      char name[OB_MAX_COLUMN_NAME_LENGTH];
      snprintf(name, sizeof(name), "c%ld", i);
      column.set_table_id(TABLE_ID);
      column.set_column_id(metas[i].column_id_);
      column.set_data_type(metas[i].obj_type_);
      column.set_collation_type(metas[i].cs_type_);
      column.set_data_length(64);
      column.set_rowkey_position(metas[i].rowkey_pos_);
      column.set_nullable(metas[i].nullable_);
      if (0 != metas[i].flags_) {
        column.set_column_flags(metas[i].flags_);
      }
      if (OB_FAIL(column.set_column_name(name))) {
        LOG_WARN("fail to set column name", K(ret), K(i));
      } else if (OB_FAIL(table_schema.add_column(column))) {
        LOG_WARN("fail to add column", K(ret), K(i));
      }
    }
    return ret;
  }

  // Seed a valid desc first so failed initialization must reset it.
  static int init_from_metas(
      const SearchTextColMeta *metas,
      const int64_t column_cnt,
      ObInvIdxRowDesc &row_desc)
  {
    int ret = OB_SUCCESS;
    share::schema::ObTableSchema table_schema;
    row_desc.reset();
    row_desc.doc_id_col_idx_ = 0;
    row_desc.index_type_ = INV_IDX_TYPE_FTS;
    if (OB_FAIL(build_schema(metas, column_cnt, table_schema,
        column_cnt == SEARCH_TEXT_COL_CNT_WITH_POS
            ? share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL
            : share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL))) {
      LOG_WARN("fail to build search text schema", K(ret));
    } else {
      ret = row_desc.init_search_text(table_schema);
    }
    return ret;
  }

  static int64_t tf_storage_idx()
  {
    return SEARCH_TEXT_ROWKEY_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  }

  static void assert_reset(const ObInvIdxRowDesc &row_desc)
  {
    ASSERT_EQ(-1, row_desc.doc_id_col_idx_);
    ASSERT_EQ(-1, row_desc.token_freq_col_idx_);
    ASSERT_EQ(-1, row_desc.doc_length_col_idx_);
    ASSERT_EQ(-1, row_desc.pos_list_col_idx_);
    ASSERT_EQ(-1, row_desc.nested_key_col_idx_);
    ASSERT_EQ(INV_IDX_TYPE_INVALID, row_desc.index_type_);
  }
};

TEST_F(TestSearchTextRowDesc, resolve_match_and_phrase_layout_without_flags)
{
  SearchTextColMeta metas[MAX_COL_CNT];
  share::schema::ObTableSchema index_schema;
  share::schema::ObTableSchema parent_schema;
  ObInvIdxRowDesc row_desc;
  ObInvIdxRowDesc dispatched_desc;

  // MATCH: 6 logical columns, no position list.
  int64_t column_cnt = init_col_metas(false, metas);
  ASSERT_EQ(OB_SUCCESS, build_schema(metas, column_cnt, index_schema,
      share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL));
  ASSERT_EQ(OB_SUCCESS, row_desc.init_search_text(index_schema));
  ASSERT_EQ(SEARCH_TEXT_DOC_ID, row_desc.doc_id_col_idx_);
  ASSERT_EQ(tf_storage_idx(), row_desc.token_freq_col_idx_);
  ASSERT_EQ(tf_storage_idx() + 1, row_desc.doc_length_col_idx_);
  ASSERT_EQ(-1, row_desc.pos_list_col_idx_);
  ASSERT_EQ(-1, row_desc.nested_key_col_idx_);
  ASSERT_EQ(INV_IDX_TYPE_SEARCH_TEXT, row_desc.index_type_);
  ASSERT_FALSE(row_desc.has_posting_value());

  // The parent schema has none of the text column ids, so init() must not consult it.
  parent_schema.set_tenant_id(1);
  parent_schema.set_table_id(9999);
  ASSERT_EQ(OB_SUCCESS, dispatched_desc.init(index_schema, parent_schema));
  ASSERT_EQ(row_desc.doc_id_col_idx_, dispatched_desc.doc_id_col_idx_);
  ASSERT_EQ(row_desc.token_freq_col_idx_, dispatched_desc.token_freq_col_idx_);
  ASSERT_EQ(row_desc.doc_length_col_idx_, dispatched_desc.doc_length_col_idx_);
  ASSERT_EQ(row_desc.pos_list_col_idx_, dispatched_desc.pos_list_col_idx_);
  ASSERT_EQ(INV_IDX_TYPE_SEARCH_TEXT, dispatched_desc.index_type_);

  // PHRASE: 7 logical columns, position list as the only posting value.
  column_cnt = init_col_metas(true, metas);
  ASSERT_EQ(OB_SUCCESS, build_schema(metas, column_cnt, index_schema,
      share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL));
  ASSERT_EQ(OB_SUCCESS, row_desc.init_search_text(index_schema));
  ASSERT_EQ(SEARCH_TEXT_DOC_ID, row_desc.doc_id_col_idx_);
  ASSERT_EQ(tf_storage_idx(), row_desc.token_freq_col_idx_);
  ASSERT_EQ(tf_storage_idx() + 1, row_desc.doc_length_col_idx_);
  ASSERT_EQ(tf_storage_idx() + 2, row_desc.pos_list_col_idx_);
  ASSERT_EQ(-1, row_desc.nested_key_col_idx_);
  ASSERT_EQ(INV_IDX_TYPE_SEARCH_TEXT, row_desc.index_type_);
  ASSERT_TRUE(row_desc.has_posting_value());
  ASSERT_EQ(OB_SUCCESS, dispatched_desc.init(index_schema, parent_schema));
  ASSERT_EQ(row_desc.pos_list_col_idx_, dispatched_desc.pos_list_col_idx_);
}

TEST_F(TestSearchTextRowDesc, data_writer_decodes_versioned_and_legacy_value_configs)
{
  using namespace share::schema;
  for (int variant = 0; variant < 5; ++variant) {
    const bool nested = variant >= 2;
    ObSearchIndexConfig config;
    ObSearchIndexModeConfig value;
    value.filter_ = variant == 2 ? ObString("NESTED_PATHS=7$.items") : ObString();
    if (variant == 2) {
      ASSERT_EQ(OB_SUCCESS, config.decode(value.filter_));
    } else {
      ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
    }
    if (nested && variant != 2) {
      ASSERT_EQ(OB_SUCCESS, config.init(config.get_modes(), ObString("NESTED_PATHS=7$.items")));
    }
    if (variant == 1) {
      ObSearchIndexModeConfig fulltext;
      fulltext.mode_ = ObSearchIndexModeConfig::FULLTEXT_MATCH;
      fulltext.filter_ = ObString("INCLUDE_TYPES=11json_string");
      fulltext.parser_name_ = ObString("space.1");
      fulltext.parser_properties_ = ObString("{\"min_token_size\":3,\"max_token_size\":84}");
      ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext));
    }
    ObSqlString comment;
    if (variant == 2) {
      ASSERT_EQ(OB_SUCCESS, comment.append(value.filter_));
    } else if (variant == 4) {
      ASSERT_EQ(OB_SUCCESS, config.encode(comment));
    } else {
      ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, comment));
    }
    ObTableSchema def;
    def.set_tenant_id(1);
    def.set_table_id(9999);
    def.set_table_type(USER_INDEX);
    def.set_index_type(INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL);
    ObColumnSchemaV2 source;
    source.set_tenant_id(1);
    source.set_table_id(9999);
    source.set_column_id(101);
    source.set_data_type(ObJsonType);
    source.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    source.set_data_length(1024);
    source.set_index_position(1);
    ASSERT_EQ(OB_SUCCESS, source.set_column_name("payload"));
    ASSERT_EQ(OB_SUCCESS, source.set_comment(comment.string()));
    ASSERT_EQ(OB_SUCCESS, def.add_column(source));

    SearchTextColMeta metas[MAX_COL_CNT];
    init_col_metas(false, metas);
    metas[SEARCH_INDEX_COL_IDX].obj_type_ = ObIntType;
    metas[SEARCH_INDEX_NESTED_KEY].obj_type_ = ObVarcharType;
    ObTableSchema data;
    ASSERT_EQ(OB_SUCCESS, build_schema(metas, SEARCH_INDEX_KEY_CNT + (nested ? 1 : 0),
        data, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL));
    ObInvIdxRowDesc row_desc;
    ASSERT_EQ(OB_SUCCESS, row_desc.init(data, def));
    EXPECT_EQ(INV_IDX_TYPE_SEARCH, row_desc.index_type_);
    EXPECT_EQ(nested ? tf_storage_idx() : -1, row_desc.nested_key_col_idx_);

    ASSERT_EQ(OB_SUCCESS, def.get_column_schema(101)->set_comment(
        ObString("SEARCH_INDEX_CONFIG=1:1:0:3:0:0:0:")));
    EXPECT_EQ(OB_INVALID_ARGUMENT, row_desc.init(data, def));
    assert_reset(row_desc);
  }
}

TEST_F(TestSearchTextRowDesc, ordinary_columns_with_non_contiguous_ids_survive_reload)
{
  for (const bool phrase : {false, true}) {
    SearchTextColMeta metas[MAX_COL_CNT];
    const int64_t column_cnt = init_col_metas(phrase, metas);
    for (int64_t i = 0; i < column_cnt; ++i) {
      metas[i].column_id_ = OB_APP_MIN_COLUMN_ID + 20 + 3 * i;
    }
    share::schema::ObTableSchema schema;
    ASSERT_EQ(OB_SUCCESS, build_schema(metas, column_cnt, schema));
    ObArenaAllocator allocator;
    const int64_t size = schema.get_serialize_size();
    char *buffer = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, buffer);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, schema.serialize(buffer, size, pos));
    share::schema::ObTableSchema restored;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(buffer, size, pos));
    for (int64_t i = 0; i < column_cnt; ++i) {
      const auto *column = restored.get_column_schema(metas[i].column_id_);
      ASSERT_NE(nullptr, column);
      EXPECT_EQ(0, column->get_column_flags());
    }
    ObInvIdxRowDesc row_desc;
    ASSERT_EQ(OB_SUCCESS, row_desc.init_search_text(restored));
    EXPECT_EQ(SEARCH_TEXT_DOC_ID, row_desc.doc_id_col_idx_);
    EXPECT_EQ(tf_storage_idx(), row_desc.token_freq_col_idx_);
    EXPECT_EQ(tf_storage_idx() + 1, row_desc.doc_length_col_idx_);
    EXPECT_EQ(phrase ? tf_storage_idx() + 2 : -1, row_desc.pos_list_col_idx_);
  }
}

TEST_F(TestSearchTextRowDesc, doc_id_is_resolved_by_position_without_flag)
{
  SearchTextColMeta metas[MAX_COL_CNT];
  ObInvIdxRowDesc row_desc;
  const int64_t column_cnt = init_col_metas(true, metas);

  // Heap hidden primary key as DOC_ID, without any generated column flag.
  metas[SEARCH_TEXT_DOC_ID].column_id_ = OB_HIDDEN_PK_INCREMENT_COLUMN_ID;
  ASSERT_EQ(OB_SUCCESS, init_from_metas(metas, column_cnt, row_desc));
  ASSERT_EQ(SEARCH_TEXT_DOC_ID, row_desc.doc_id_col_idx_);
  ASSERT_EQ(tf_storage_idx() + 2, row_desc.pos_list_col_idx_);

  // An explicit DOC_ID flag does not change the position-based resolution.
  metas[SEARCH_TEXT_DOC_ID].column_id_ = OB_APP_MIN_COLUMN_ID + 3;
  metas[SEARCH_TEXT_DOC_ID].flags_ = GENERATED_DOC_ID_COLUMN_FLAG;
  ASSERT_EQ(OB_SUCCESS, init_from_metas(metas, column_cnt, row_desc));
  ASSERT_EQ(SEARCH_TEXT_DOC_ID, row_desc.doc_id_col_idx_);
  ASSERT_EQ(tf_storage_idx(), row_desc.token_freq_col_idx_);
}

TEST_F(TestSearchTextRowDesc, reject_index_type_and_column_count_mismatch)
{
  for (int64_t with_pos = 0; with_pos < 2; ++with_pos) {
    SearchTextColMeta metas[MAX_COL_CNT];
    const int64_t column_cnt = init_col_metas(0 != with_pos, metas);
    share::schema::ObTableSchema schema;
    // Deliberately use MATCH for seven columns and PHRASE for six columns.
    ASSERT_EQ(OB_SUCCESS, build_schema(metas, column_cnt, schema, 0 != with_pos
        ? share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL
        : share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL));
    ObInvIdxRowDesc row_desc;
    row_desc.doc_id_col_idx_ = 0;
    row_desc.index_type_ = INV_IDX_TYPE_FTS;
    ASSERT_EQ(OB_ERR_UNEXPECTED, row_desc.init_search_text(schema));
    assert_reset(row_desc);
  }
}

TEST_F(TestSearchTextRowDesc, value_flags_do_not_override_physical_positions)
{
  SearchTextColMeta metas[MAX_COL_CNT];
  const int64_t column_cnt = init_col_metas(true, metas);
  metas[SEARCH_TEXT_TOKEN_FREQ].flags_ = GENERATED_FTS_DOC_LENGTH_COLUMN_FLAG;
  metas[SEARCH_TEXT_DOC_LENGTH].flags_ = GENERATED_FTS_WORD_COUNT_COLUMN_FLAG;
  ObInvIdxRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_from_metas(metas, column_cnt, row_desc));
  EXPECT_EQ(tf_storage_idx(), row_desc.token_freq_col_idx_);
  EXPECT_EQ(tf_storage_idx() + 1, row_desc.doc_length_col_idx_);
  EXPECT_EQ(tf_storage_idx() + 2, row_desc.pos_list_col_idx_);
}

TEST_F(TestSearchTextRowDesc, accepts_nullable_schema_and_signed_term_prefix)
{
  for (int64_t with_pos = 0; with_pos < 2; ++with_pos) {
    SearchTextColMeta metas[MAX_COL_CNT];
    const int64_t column_cnt = init_col_metas(0 != with_pos, metas);
    metas[SEARCH_TEXT_COL_IDX].obj_type_ = ObIntType;
    for (int64_t i = 0; i < column_cnt; ++i) {
      metas[i].nullable_ = true;
    }
    ObInvIdxRowDesc row_desc;
    ASSERT_EQ(OB_SUCCESS, init_from_metas(metas, column_cnt, row_desc));
    EXPECT_EQ(SEARCH_TEXT_DOC_ID, row_desc.doc_id_col_idx_);
    EXPECT_EQ(tf_storage_idx(), row_desc.token_freq_col_idx_);
    EXPECT_EQ(tf_storage_idx() + 1, row_desc.doc_length_col_idx_);
    EXPECT_EQ(0 != with_pos, row_desc.pos_list_col_idx_ >= 0);

    // Term comparison follows the schema collation, not the posting binary encoder.
    metas[SEARCH_TEXT_PATH].cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
    metas[SEARCH_TEXT_TOKEN].cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
    ASSERT_EQ(OB_SUCCESS, init_from_metas(metas, column_cnt, row_desc));
  }
}

TEST_F(TestSearchTextRowDesc, reject_layout_and_posting_type_mismatch)
{
  SearchTextColMeta metas[MAX_COL_CNT];
  ObInvIdxRowDesc row_desc;
  share::schema::ObTableSchema fts_schema;
  const int64_t tf_idx = SEARCH_TEXT_ROWKEY_CNT;
  const int64_t dl_idx = SEARCH_TEXT_ROWKEY_CNT + 1;
  const int64_t pos_idx = SEARCH_TEXT_ROWKEY_CNT + 2;
  int64_t column_cnt = 0;

  // PHRASE position list must have the binary string encoding type.
  column_cnt = init_col_metas(true, metas);
  metas[pos_idx].obj_type_ = ObUInt64Type;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);

  // 6 logical columns carrying POS instead of DL.
  column_cnt = init_col_metas(false, metas);
  metas[dl_idx].obj_type_ = ObVarcharType;
  metas[dl_idx].flags_ = GENERATED_FTS_POS_LIST_COLUMN_FLAG;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);

  // 5 and 8 logical columns.
  column_cnt = init_col_metas(false, metas) - 1;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);
  column_cnt = init_col_metas(true, metas);
  metas[column_cnt] = metas[pos_idx];
  metas[column_cnt].column_id_ = OB_APP_MIN_COLUMN_ID + column_cnt;
  ++column_cnt;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);

  // DOC_ID must be the last business key, not TOKEN.
  column_cnt = init_col_metas(false, metas);
  metas[SEARCH_TEXT_TOKEN].rowkey_pos_ = SEARCH_TEXT_DOC_ID + 1;
  metas[SEARCH_TEXT_DOC_ID].rowkey_pos_ = SEARCH_TEXT_TOKEN + 1;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);

  // 3 business keys: DOC_ID is not part of the rowkey.
  column_cnt = init_col_metas(false, metas);
  metas[SEARCH_TEXT_DOC_ID].rowkey_pos_ = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);

  // Posting values retain their encoding type requirements.
  column_cnt = init_col_metas(true, metas);
  metas[pos_idx].cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);
  column_cnt = init_col_metas(false, metas);
  metas[SEARCH_TEXT_DOC_ID].obj_type_ = ObIntType;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);
  column_cnt = init_col_metas(false, metas);
  metas[tf_idx].obj_type_ = ObIntType;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);

  column_cnt = init_col_metas(false, metas);
  metas[dl_idx].obj_type_ = ObIntType;
  ASSERT_EQ(OB_ERR_UNEXPECTED, init_from_metas(metas, column_cnt, row_desc));
  assert_reset(row_desc);

  // A valid text layout under a non-text index type is rejected by the text initializer.
  column_cnt = init_col_metas(false, metas);
  ASSERT_EQ(OB_SUCCESS, build_schema(metas, column_cnt, fts_schema,
      share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL));
  ASSERT_EQ(OB_INVALID_ARGUMENT, row_desc.init_search_text(fts_schema));
  assert_reset(row_desc);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

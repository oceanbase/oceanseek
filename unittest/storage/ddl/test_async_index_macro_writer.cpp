/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

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

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

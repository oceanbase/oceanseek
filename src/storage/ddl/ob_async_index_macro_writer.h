/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_ASYNC_INDEX_MACRO_WRITER_H_
#define OCEANBASE_STORAGE_DDL_OB_ASYNC_INDEX_MACRO_WRITER_H_

#include "lib/string/ob_string_buffer.h"
#include "share/search_index/ob_search_index_nested_key_list.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{
class ObDatumRow;
class ObBatchDatumRows;
class ObSSTableIndexBuilder;
class ObISSTableObjectCleaner;
}
namespace storage
{
}

namespace storage
{
// Interface for async index macro block writer. The real implementation
// (writing the async index container macro block) is provided separately.
class ObAsyncIndexMacroWriter : public ObITabletSliceWriter
{
public:
  ObAsyncIndexMacroWriter() {}
  virtual ~ObAsyncIndexMacroWriter() {}
};

// Real adapter that drives ObInvIdxMacroBlockWriter so the DDL write pipeline
// produces true inverted-index macro blocks. Macro block metas are routed into
// the merge-side macro_meta_store through ObDDLRedoLogWriterCallback.
class ObAsyncIndexMacroWriterAdapter : public ObAsyncIndexMacroWriter
{
public:
  ObAsyncIndexMacroWriterAdapter();
  virtual ~ObAsyncIndexMacroWriterAdapter();
  int init(const ObWriteMacroParam &param) override;
  void reset() override;
  int append_row(const blocksstable::ObDatumRow &row) override;
  int append_batch(const blocksstable::ObBatchDatumRows &batch_rows) override;
  int64_t get_row_count() const override { return row_count_; }
  bool is_nested_key_aggregation_enabled() const
  {
    return inv_idx_row_desc_.nested_key_col_idx_ >= 0;
  }
  int close() override;
  TO_STRING_KV(K_(is_inited), K_(row_count));
private:
  int append_one_row(const blocksstable::ObDatumRow &row);
  int append_nested_search_row(const blocksstable::ObDatumRow &row);
  int start_nested_group(const blocksstable::ObDatumRow &row);
  int append_nested_key(const blocksstable::ObStorageDatum &nested_key);
  int flush_nested_group();
  void reuse_nested_group();
  bool is_same_posting(const blocksstable::ObDatumRow &row) const;
  int build_inv_idx_row_desc(const ObWriteMacroParam &param,
                             blocksstable::ObInvIdxRowDesc &inv_idx_row_desc);
  static int build_search_inv_idx_row_desc(
      const bool has_nested_paths,
      const int64_t rowkey_column_cnt,
      const int64_t storage_column_cnt,
      blocksstable::ObInvIdxRowDesc &inv_idx_row_desc);
  int prepare_data_store_desc(const ObWriteMacroParam &param,
                              const blocksstable::ObInvIdxRowDesc &inv_idx_row_desc);
  int prepare_callback(const ObWriteMacroParam &param);
private:
  bool is_inited_;
  ObArenaAllocator allocator_;
  ObArenaAllocator nested_group_allocator_;
  common::ObStringBuffer nested_key_list_buffer_;
  share::ObSearchIndexNestedKeyListWriter nested_key_list_writer_;
  blocksstable::ObDatumRow pending_nested_row_;
  blocksstable::ObInvIdxRowDesc inv_idx_row_desc_;
  blocksstable::ObWholeDataStoreDesc data_desc_;
  blocksstable::ObSSTableIndexBuilder *index_builder_;
  blocksstable::ObISSTableObjectCleaner *object_cleaner_; // owned by index_builder_, not freed here
  blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback_;
  bool is_inc_redo_callback_;
  blocksstable::ObInvIdxMacroBlockWriter inv_writer_;
  ObTabletID tablet_id_;
  int64_t cg_idx_;
  int64_t parallel_idx_;
  int64_t row_count_;
  bool has_pending_nested_group_;
  DISALLOW_COPY_AND_ASSIGN(ObAsyncIndexMacroWriterAdapter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_DDL_OB_ASYNC_INDEX_MACRO_WRITER_H_

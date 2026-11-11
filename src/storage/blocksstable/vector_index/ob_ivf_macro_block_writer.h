/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MACRO_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MACRO_BLOCK_WRITER_H_

#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"

namespace oceanbase
{
namespace blocksstable
{

// 不同 IVF 行类型（centroid / full-precision vector / posting）不能写入同一微块。
// 使用带 row_desc 的 open() 后，append_row()/append_batch() 会在类型切换时自动刷微块。
class ObIvfMacroBlockWriter : public ObMacroBlockWriter
{
public:
  ObIvfMacroBlockWriter();
  virtual ~ObIvfMacroBlockWriter();
  virtual void reset() override;
  int open(
      const ObDataStoreDesc &data_store_desc,
      const int64_t parallel_idx,
      const blocksstable::ObMacroSeqParam &macro_seq_param,
      const share::ObPreWarmerParam &pre_warm_param,
      ObISSTableObjectCleaner &object_cleaner,
      ObIMacroBlockFlushCallback *callback = nullptr,
      ObIMacroBlockValidator *validator = nullptr,
      ObIODevice *device_handle = nullptr) override;
  int open(
      const ObIvfSSTableRowDesc &row_desc,
      const ObDataStoreDesc &data_store_desc,
      const int64_t parallel_idx,
      const blocksstable::ObMacroSeqParam &macro_seq_param,
      const share::ObPreWarmerParam &pre_warm_param,
      ObISSTableObjectCleaner &object_cleaner,
      ObIMacroBlockFlushCallback *callback = nullptr,
      ObIMacroBlockValidator *validator = nullptr,
      ObIODevice *device_handle = nullptr);
  int append_row(
      const ObDatumRow &row,
      const ObMacroBlockDesc *curr_macro_desc = nullptr) override;
  int append_batch(
      const ObBatchDatumRows &datum_rows,
      const ObMacroBlockDesc *curr_macro_desc = nullptr) override;
  int flush_micro_block();
  int64_t get_appended_row_count() const { return appended_row_count_; }
private:
  int inner_append_row(
      const ObDatumRow &row,
      const ObMacroBlockDesc *curr_macro_desc);
  int inner_flush_micro_block();
private:
  ObIvfSSTableRowDesc row_desc_;
  int64_t previous_row_type_;
  int64_t appended_row_count_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MACRO_BLOCK_WRITER_H_

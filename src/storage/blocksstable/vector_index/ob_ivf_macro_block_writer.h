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
  // Pack small posting lists across CIDs at the schema target. Larger lists
  // use a byte-based adaptive target, bounded by the storage/encoder limit.
  explicit ObIvfMacroBlockWriter(int64_t posting_micro_block_size_limit = INT64_MAX);
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
  // Borrow counts through close/reset. Entries are indexed by storage CID
  // minus first_cid, and include secondary postings. No cardinality prepass.
  int set_posting_counts(const common::ObIArray<uint32_t> &counts, int64_t first_cid);
  int64_t get_appended_row_count() const { return appended_row_count_; }
private:
  int inner_append_row(
      const ObDatumRow &row,
      const ObMacroBlockDesc *curr_macro_desc);
  int inner_flush_micro_block();
  int64_t posting_size_limit() const;
  int64_t get_posting_count(int64_t cid) const;
  int64_t estimate_posting_target(int64_t row_count, int64_t row_size) const;
  int64_t update_posting_target(const ObDatumRow &row, int64_t cid);
private:
  ObIvfSSTableRowDesc row_desc_;
  // Only the soft micro-block target varies; shared schema/physical identity
  // remain borrowed from the caller for the lifetime of this writer.
  ObDataStoreDesc ivf_data_store_desc_;
  int64_t schema_micro_block_size_;
  int64_t previous_row_type_;
  int64_t previous_posting_cid_;
  int64_t appended_row_count_;
  const common::ObIArray<uint32_t> *posting_counts_;
  int64_t first_posting_cid_;
  int64_t posting_split_size_;
  int64_t posting_cluster_payload_bytes_;
  bool posting_cluster_is_large_;
  const int64_t posting_micro_block_size_limit_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MACRO_BLOCK_WRITER_H_

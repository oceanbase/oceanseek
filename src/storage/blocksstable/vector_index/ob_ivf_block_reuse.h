/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BLOCK_REUSE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BLOCK_REUSE_H_

#include "storage/blocksstable/vector_index/ob_ivf_block_reuse_source.h"
#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"

namespace oceanbase
{
namespace blocksstable
{
class ObIvfSSTable;
class ObIvfMacroBlockWriter;
class ObDataStoreDesc;

// Consumes the existing COMPLETE ordered output stream, substituting certified
// unchanged spans with old macro references or compressed micro copies. This
// reduces physical writes/encoding. Certified RAW keys can additionally drive
// reuse before their payloads are materialized by the output reader.
// Each task has one active source cursor. Sorted, disjoint ranges select the
// original input slot; the source and range arrays outlive finish().
class ObIVFBlockReuseWriter final
{
public:
  explicit ObIVFBlockReuseWriter(common::ObIAllocator &allocator);
  ~ObIVFBlockReuseWriter();
  static int check_source(const ObIVFBlockReuseSource &source, const ObDataStoreDesc &desc, bool &compatible);
  int init(const common::ObIArray<ObIVFBlockReuseSource *> &sources, const common::ObIArray<ObIVFReuseRange> &ranges,
      const ObIVFReuseRange &task_range, const ObDataStoreDesc &desc, ObIvfMacroBlockWriter &writer);
  int append_row(const ObDatumRow &row);
  // Consume a complete certified RAW candidate using the already validated
  // ordered key stream. No assumption is made about consecutive integer VIDs.
  // end is the end of the complete task slice, not a payload batch boundary.
  // If no candidate starts here, read_end bounds the ordinary payload prefix.
  int try_reuse_raw_keys(const common::ObIArray<uint64_t> &vids,
      int64_t begin, int64_t end, int64_t &reused_rows, int64_t &read_end);
  int finish();
  int64_t row_count() const { return rows_; }
  int64_t reused_macros() const { return reused_macros_; }
  int64_t copied_micros() const { return copied_micros_; }

private:
  bool eligible(const ObDatumRowkey &first, const ObDatumRowkey &last) const;
  bool may_reuse(const ObDatumRowkey &first, const ObDatumRowkey &last) const;
  bool compatible_macro() const;
  int open_next_range();
  int next_candidate();
  int write_candidate();
  int copy_micro(const ObMicroIndexInfo &index, const ObDatumRowkey &end);
  void destroy_iter(ObIMacroBlockIterator *&iter, common::ObIAllocator &allocator);
  common::ObIAllocator &allocator_;
  common::ObArenaAllocator range_index_allocator_;
  common::ObArenaAllocator micro_index_allocator_;
  const common::ObIArray<ObIVFBlockReuseSource *> *sources_;
  common::ObArray<int8_t, common::ObWrapperAllocator> source_compatible_;
  const ObIVFBlockReuseSource *source_;
  const common::ObIArray<ObIVFReuseRange> *ranges_;
  const ObDataStoreDesc *desc_;
  ObIvfMacroBlockWriter *writer_;
  ObIVFReuseRange task_range_;
  ObIVFReuseRange active_range_;
  int64_t range_pos_;
  ObDatumRange scan_range_;
  ObStorageDatum start_[3], end_[3];
  ObIMacroBlockIterator *macro_iter_, *micro_iter_;
  ObMacroBlockDesc macro_;
  ObDataMacroBlockMeta meta_;
  int64_t micro_pos_;
  ObDatumRowkey candidate_end_;
  ObIVFReuseKey first_, last_, last_output_;
  int64_t candidate_rows_, consumed_, rows_, reused_macros_, copied_micros_;
  int64_t macro_payload_bytes_, micro_read_bytes_;
  bool candidate_, macro_candidate_, ended_, has_output_, finished_;
  int error_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFBlockReuseWriter);
};
} // namespace blocksstable
} // namespace oceanbase
#endif

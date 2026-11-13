/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inv_idx_aggregator.h"
#include "ob_inv_sstable_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"

namespace oceanbase
{
namespace blocksstable
{

void ObInvIdxAggregator::reset()
{
  agg_info_.init_aggregate();
  has_rows_ = false;
}

int ObInvIdxAggregator::eval(const ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  ObInvIdxAggInfo child;
  int64_t pos = 0;
  if (ObIndexRowAggType::INVERTED_INDEX_AGG == row_desc.agg_type_ && !row_desc.is_data_block_) {
    if (OB_FAIL(ObInvIdxAggInfoSerializer::deserialize(
                row_desc.inverted_index_agg_.buf_, row_desc.inverted_index_agg_.size_, pos, child))) {
      LOG_WARN("failed to decode inverted index payload", K(ret), K(row_desc));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(agg_info_.merge(child))) {
    LOG_WARN("failed to aggregate inverted bm25 stats", K(ret), K(row_desc));
  } else {
    has_rows_ = has_rows_ || row_desc.row_count_ > 0;
  }
  return ret;
}

int ObInvIdxAggregator::fill_row_desc(ObIndexBlockRowDesc &row_desc, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (has_rows_ && OB_FAIL(fill_row_desc(agg_info_, row_desc, allocator))) {
    LOG_WARN("failed to fill inverted bm25 stats", K(ret));
  }
  return ret;
}

int ObInvIdxAggregator::fill_row_desc(
    const ObInvIdxAggInfo &summary, ObIndexBlockRowDesc &row_desc,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  int64_t pos = 0;
  char *buf = nullptr;
  if (summary.empty()) {
  } else if (OB_FAIL(ObInvIdxAggInfoSerializer::get_serialize_size(summary, size))) {
    LOG_WARN("failed to get inverted statistics size", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate inverted statistics buffer", K(ret), K(size));
  } else if (OB_FAIL(ObInvIdxAggInfoSerializer::serialize(summary, buf, size, pos))) {
    LOG_WARN("failed to encode inverted statistics", K(ret), K(size));
  } else {
    row_desc.inverted_index_agg_ = {buf, pos};
    row_desc.agg_type_ = ObIndexRowAggType::INVERTED_INDEX_AGG;
  }
  if (OB_FAIL(ret) && nullptr != buf) {
    allocator.free(buf);
  }
  return ret;
}

int ObInvIdxAggregator::get_summary_from_macro(const ObInvMacroMeta &macro_meta, ObInvIdxAggInfo &summary)
{
  int ret = OB_SUCCESS;
  summary.reset();
  if (OB_FAIL(summary.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, macro_meta.get_stats().total_doc_length_))) {
    LOG_WARN("failed to set inverted macro length", K(ret));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

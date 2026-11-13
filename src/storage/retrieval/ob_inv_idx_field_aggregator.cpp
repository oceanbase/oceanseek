/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inv_idx_field_aggregator.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "storage/access/ob_index_block_tree_traverser.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_decoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_info.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

// Exact document length aggregation over the same cached index tree used by row estimation.
class ObInvIdxLengthContext final : public ObIIndexTreeTraverserContext
{
public:
  ObInvIdxLengthContext(const ObDatumRange &range, const ObDatumRowkey *field_prefix,
                       const ObStorageDatumUtils &datum_utils, ObStorageDatum &total_doc_length)
    : ObIIndexTreeTraverserContext(true /*is_never_estimate*/, false /*consider_multi_version*/),
      range_(range),
      field_prefix_(field_prefix),
      datum_utils_(datum_utils),
      total_doc_length_(total_doc_length),
      is_ended_(false)
  {}

  bool is_valid() const override { return range_.is_valid(); }
  bool is_ended() const override { return is_ended_; }
  const ObDatumRange &get_curr_range() const override { return range_; }
  int next_range() override { is_ended_ = true; return OB_SUCCESS; }

  int on_inner_node(const ObMicroIndexInfo &index_row,
                    const bool is_coverd_by_range,
                    ObTraverserOperationType &operation) override
  {
    int ret = OB_SUCCESS;
    operation = GOTO_NEXT_LEVEL;
    if (!index_row.is_data_block() && is_coverd_by_range && index_row.row_header_->is_inverted_index()) {
      ObInvIdxAggInfo stats;
      int64_t pos = 0;
      number::ObNumber length;
      if (OB_FAIL(ObInvIdxAggInfoSerializer::deserialize(index_row.agg_row_buf_, index_row.agg_buf_size_, pos, stats))) {
        STORAGE_LOG(WARN, "failed to read inverted subtree statistics", K(ret), K(index_row));
      } else if (OB_FAIL(stats.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "failed to get inverted subtree length", K(ret), K(index_row));
        }
      } else {
        operation = NOTHING;
        if (OB_FAIL(add_length_(length))) {
          STORAGE_LOG(WARN, "failed to add inverted subtree length", K(ret), K(index_row));
        }
      }
    }
    return ret;
  }

  int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range) override
  {
    int ret = OB_SUCCESS;
    int cmp = 0;
    uint64_t length = 0;
    // Each inverted leaf index row belongs to exactly one term, even when
    // several terms share a posting micro block. Restrict COL_IDX/PATH only for Search Text.
    if (nullptr != field_prefix_ && !is_coverd_by_range
        && OB_FAIL(index_row.endkey_.compare(*field_prefix_, datum_utils_, cmp, false /*compare_datum_cnt*/))) {
      STORAGE_LOG(WARN, "failed to compare search text field prefix", K(ret), K(index_row));
    } else if (0 != cmp) {
      // Range location can include a boundary leaf row outside the requested field.
    } else if (OB_UNLIKELY(!index_row.row_header_->is_inverted_index())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "inverted leaf index row has no logical block statistics", K(ret), K(index_row));
    } else if (OB_FAIL(ObInvIdxAggDataDecoder::get_total_doc_length(index_row.agg_row_buf_, index_row.agg_buf_size_, length))) {
      STORAGE_LOG(WARN, "failed to read inverted leaf length", K(ret), K(index_row));
    } else {
      sql::ObNumStackAllocator<1> allocator;
      number::ObNumber leaf_length;
      if (OB_FAIL(leaf_length.from(length, allocator))) {
        STORAGE_LOG(WARN, "failed to convert leaf document length", K(ret), K(length));
      } else if (OB_FAIL(add_length_(leaf_length))) {
        STORAGE_LOG(WARN, "failed to add leaf document length", K(ret), K(length));
      }
    }
    return ret;
  }

  int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est) override
  {
    UNUSED(est);
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "document length aggregation must not open posting blocks", K(ret), K(index_row));
    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(range), K_(total_doc_length), K_(is_ended));

private:
  int add_length_(const number::ObNumber &length)
  {
    int ret = OB_SUCCESS;
    sql::ObNumStackAllocator<1> allocator;
    number::ObNumber current(total_doc_length_.get_number());
    number::ObNumber sum;
    if (OB_FAIL(current.add_v3(length, sum, allocator))) {
      STORAGE_LOG(WARN, "failed to add document lengths", K(ret), K_(total_doc_length), K(length));
    } else {
      total_doc_length_.set_number(sum);
    }
    return ret;
  }

  const ObDatumRange &range_;
  const ObDatumRowkey *field_prefix_;
  const ObStorageDatumUtils &datum_utils_;
  ObStorageDatum &total_doc_length_;
  bool is_ended_;
};

int ObInvIdxFieldAggregator::aggregate_total_doc_length(
    ObSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObDatumRowkey *field_prefix,
    ObTablet *tablet,
    ObStorageDatum &total_doc_length)
{
  int ret = OB_SUCCESS;
  total_doc_length.set_number(number::ObNumber::get_zero());
  const auto &cols = read_info.get_columns_desc();
  const int64_t prefix_count = nullptr == field_prefix ? 0 : SEARCH_TEXT_TOKEN;
  const int64_t rowkey_count = prefix_count + 2;
  if (OB_UNLIKELY(!sstable.is_valid() || !sstable.is_inv_sstable() || !read_info.is_valid()
      || (nullptr != field_prefix
          && (!field_prefix->is_valid() || field_prefix->get_datum_cnt() != SEARCH_TEXT_TOKEN)))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inverted length aggregation arguments", K(ret), K(sstable), K(read_info), KPC(field_prefix));
  } else if (OB_UNLIKELY(!static_cast<ObInvSSTable &>(sstable).get_inv_meta().get_stats().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid inverted statistics", K(ret), K(sstable));
  } else if (OB_UNLIKELY((nullptr != field_prefix) != static_cast<ObInvSSTable &>(sstable).get_inv_meta().get_stats().is_search_text_index()
             || rowkey_count != read_info.get_schema_rowkey_count() || cols.count() < rowkey_count
             || (nullptr != field_prefix
                 && (!cols.at(SEARCH_TEXT_COL_IDX).col_type_.is_uint64()
                     || ObVarcharType != cols.at(SEARCH_TEXT_PATH).col_type_.get_type())))) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "document length aggregation layout mismatch", K(ret), K(sstable), K(read_info), KPC(field_prefix), K(rowkey_count));
  } else if (!sstable.is_empty()) {
    ObStorageDatum start[SEARCH_TEXT_ROWKEY_CNT];
    ObStorageDatum end[SEARCH_TEXT_ROWKEY_CNT];
    for (int64_t i = 0; i < prefix_count; ++i) {
      start[i] = field_prefix->get_datum(i);
      end[i] = field_prefix->get_datum(i);
    }
    for (int64_t i = prefix_count; i < rowkey_count; ++i) {
      start[i].set_min();
      end[i].set_max();
    }
    ObDatumRange range;
    ObIndexBlockTreeTraverser traverser;
    if (OB_FAIL(range.get_start_key().assign(start, rowkey_count))) {
      STORAGE_LOG(WARN, "failed to assign document length range start", K(ret));
    } else if (OB_FAIL(range.get_end_key().assign(end, rowkey_count))) {
      STORAGE_LOG(WARN, "failed to assign document length range end", K(ret));
    } else if (OB_FAIL(traverser.init(sstable, read_info, tablet, "InvFieldDL"))) {
      STORAGE_LOG(WARN, "failed to initialize document length traversal", K(ret));
    } else {
      range.set_left_closed();
      range.set_right_closed();
      ObStorageDatum sum;
      sum.set_number(number::ObNumber::get_zero());
      ObInvIdxLengthContext context(range, field_prefix, read_info.get_datum_utils(), sum);
      if (OB_FAIL(traverser.traverse(context))) {
        STORAGE_LOG(WARN, "failed to aggregate inverted document length", K(ret), KPC(field_prefix));
      } else {
        total_doc_length = sum;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

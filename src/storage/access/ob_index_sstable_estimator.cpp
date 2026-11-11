/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_index_sstable_estimator.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ob_trans_version_skip_index_util.h"
#include "storage/access/ob_index_block_tree_traverser.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
using namespace blocksstable;
ObPartitionEst::ObPartitionEst()
    : logical_row_count_(0),
      physical_row_count_(0)
{
}

int ObPartitionEst::add(const ObPartitionEst &pe)
{
  int ret = common::OB_SUCCESS;

  logical_row_count_ += pe.logical_row_count_;
  physical_row_count_ += pe.physical_row_count_;

  return ret;
}


int ObPartitionEst::deep_copy(const ObPartitionEst &src)
{
  int ret = common::OB_SUCCESS;

  logical_row_count_ = src.logical_row_count_;
  physical_row_count_ = src.physical_row_count_;

  return ret;
}

ObIndexBlockScanEstimator::ObIndexBlockScanEstimator(const ObIndexSSTableEstimateContext &context)
  : context_(context)
{
  tenant_id_ = MTL_ID();
}

int ObIndexBlockScanEstimator::estimate_row_count(ObSSTable &sstable,
    const blocksstable::ObDatumRange &datum_range,
    ObPartitionEst &part_est)
{
  int ret = OB_SUCCESS;
  if (sstable.is_inv_sstable()) {
    // Inverted index sstable: estimate via the inv-specific index-tree traversal (no posting
    // data IO). deletion is recorded in deletion tablet, so logical row count set as phyusical row coun
    // here for single sstable.
    int64_t row_count = 0;
    if (OB_FAIL(estimate_inv_sstable_row_count(
            sstable, context_.index_read_info_, datum_range,
            context_.tablet_handle_.get_obj(), row_count))) {
      STORAGE_LOG(WARN, "Failed to estimate inv sstable row count", K(ret));
    } else {
      part_est.physical_row_count_ = part_est.logical_row_count_ = row_count;
    }
    STORAGE_LOG(DEBUG, "estimate inv row count result", K(ret), K(part_est));
  } else if (sstable.is_ivf_sstable()) {
    // Async IVF index sstable (both IVF_INDEX_SSTABLE and INC_IVF_INDEX_SSTABLE): estimate via the
    // IVF-specific index-tree traversal (no data micro block IO). The low precision vector data
    // micro blocks may be FastScan encoded and carry no row-format micro header, so they can not
    // be opened for estimation at all, see estimate_ivf_sstable_result. Deletion is recorded in the
    // deletion tablet, so the logical row count is the same as the physical one for a single sstable.
    int64_t row_count = 0;
    if (OB_FAIL(estimate_ivf_sstable_row_count(
            sstable, context_.index_read_info_, datum_range,
            context_.tablet_handle_.get_obj(), row_count))) {
      STORAGE_LOG(WARN, "Failed to estimate ivf sstable row count", K(ret));
    } else {
      part_est.physical_row_count_ = part_est.logical_row_count_ = row_count;
    }
    STORAGE_LOG(DEBUG, "estimate ivf row count result", K(ret), K(part_est));
  } else {
    ObEstimatedResult result;
    if (OB_FAIL(cal_total_estimate_result(sstable, datum_range, result))) {
      STORAGE_LOG(WARN, "Failed to get total estimate result", K(ret));
    } else {
      part_est.physical_row_count_ = result.total_row_count_;
      if (sstable.is_multi_version_minor_sstable()) {
        part_est.logical_row_count_ = result.total_row_count_delta_;
      } else {
        part_est.logical_row_count_ = part_est.physical_row_count_;
      }
    }
    STORAGE_LOG(DEBUG, "estimate row count result", K(ret), K(part_est));
  }
  return ret;
}

int ObIndexBlockScanEstimator::estimate_block_count(ObSSTable &sstable,
                                                    const blocksstable::ObDatumRange &datum_range,
                                                    int64_t &macro_block_cnt,
                                                    int64_t &micro_block_cnt)
{
  int ret = OB_SUCCESS;
  if (sstable.is_ivf_sstable()) {
    // Async IVF index sstable: count blocks over the index block tree only. The data micro blocks
    // (FastScan encoded low precision vectors) must not be opened, see estimate_ivf_sstable_result.
    ObIvfEstimateResult ivf_result;
    if (OB_FAIL(estimate_ivf_sstable_result(sstable, context_.index_read_info_, datum_range,
                                            context_.tablet_handle_.get_obj(), ivf_result))) {
      STORAGE_LOG(WARN, "Failed to estimate ivf sstable block count", K(ret));
    } else {
      macro_block_cnt = MAX(ivf_result.macro_block_cnt_, 1);
      micro_block_cnt = MAX(ivf_result.micro_block_cnt_, 1);
      STORAGE_LOG(TRACE, "estimate ivf block count result", K(ret), K(ivf_result),
                  K(macro_block_cnt), K(micro_block_cnt));
    }
  } else {
    ObEstimatedResult result(true /* for block */);
    if (OB_FAIL(cal_total_estimate_result(sstable, datum_range, result))) {
      STORAGE_LOG(WARN, "Failed to get total estimate result", K(ret));
    } else {
      macro_block_cnt = MAX(result.macro_block_cnt_, 1);
      micro_block_cnt = MAX(result.micro_block_cnt_, 1);
      STORAGE_LOG(TRACE, "estimate block count result", K(ret), K(result), K(macro_block_cnt), K(micro_block_cnt));
    }
  }
  return ret;
}

class EstimateRowContext : public ObIIndexTreeTraverserContext
{
public:
  static constexpr int64_t RANGE_ROWS_IN_AND_BORDER_RATIO_THRESHOLD = 1000;

  EstimateRowContext(ObSSTable &sstable,
                     const ObDatumRange &range,
                     const int64_t base_version,
                     const int64_t schema_rowkey_count,
                     ObEstimatedResult &result)
      : ObIIndexTreeTraverserContext(true /* is_never_estimate */,
                                     sstable.is_multi_version_minor_sstable() /* consider_multi_version */),
        sstable_(sstable), range_(range), base_version_(base_version),
        schema_rowkey_count_(schema_rowkey_count), result_(result), upper_trans_version_(INT64_MAX),
        is_ended_(false)
  {
  }

  ~EstimateRowContext() override = default;

  int init_trans_version_if_inc_major()
  {
    int ret = OB_SUCCESS;

    if (sstable_.is_inc_major_type_sstable()) {
      ObSSTableMetaHandle meta_handle;
      if (OB_FAIL(sstable_.get_meta(meta_handle))) {
        STORAGE_LOG(WARN, "Failed to get sstable meta", K(ret), K(sstable_));
      } else {
        const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
        upper_trans_version_ = basic_meta.get_upper_trans_version();
      }
    }

    return ret;
  }

  bool is_valid() const override { return true; }

  bool is_ended() const override { return is_ended_; }

  const ObDatumRange &get_curr_range() const override { return range_; }

  int next_range() override
  {
    is_ended_ = true;
    return OB_SUCCESS;
  }

  int on_inner_node(const ObMicroIndexInfo &index_row,
                    const bool is_coverd_by_range,
                    ObTraverserOperationType &operation)
  {
    int ret = OB_SUCCESS;

    bool is_filtered = false;
    if (OB_FAIL(is_filter_by_base_version(index_row, is_filtered))) {
      STORAGE_LOG(WARN, "Failed to check if filter by base version", K(ret));
    } else if (is_filtered) {
      operation = NOTHING;
    } else if (is_coverd_by_range) {
      result_.total_row_count_ += index_row.get_row_count();
      result_.total_row_count_delta_ += index_row.get_row_count_delta();
      result_.macro_block_cnt_ += index_row.get_macro_block_count();
      result_.micro_block_cnt_ += index_row.get_micro_block_count();
      operation = NOTHING;
    } else if (sstable_.is_major_sstable() && index_row.get_row_count() > 0
               && result_.total_row_count_ / index_row.get_row_count() >= RANGE_ROWS_IN_AND_BORDER_RATIO_THRESHOLD) {
      operation = NOTHING;
    } else if (result_.only_block_ && index_row.is_leaf_block()) {
      result_.total_row_count_ += index_row.get_row_count() / 2;
      result_.total_row_count_delta_ += index_row.get_row_count_delta() / 2;
      result_.micro_block_cnt_ += 1;
      operation = NOTHING;
    } else {
      operation = GOTO_NEXT_LEVEL;
    }

    return ret;
  }

  int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range)
  {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected call to on_node_estimate", K(index_row), K(is_coverd_by_range));
    return ret;
  }

  int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est)
  {
    result_.total_row_count_ += est.physical_row_count_;
    result_.total_row_count_delta_ += est.logical_row_count_;
    result_.micro_block_cnt_ += 1;
    return OB_SUCCESS;
  }

  int is_filter_by_base_version(const ObMicroIndexInfo &index_row, bool &is_filtered)
  {
    int ret = OB_SUCCESS;

    is_filtered = false;

    if (base_version_ <= 0) {
      // no base version, don't filter
    } else if (sstable_.is_major_sstable()) {
      // major sstable will never be filtered by base version
    } else if (sstable_.is_inc_major_type_sstable()) {
      // upper_trans_version does not correspond to all trans_version of rows in the sstable, but we cannot calculate it more precisely
      if (upper_trans_version_ <= base_version_) {
        is_filtered = true;
      }
    } else {
      ObTransVersionSkipIndexReader reader;
      ObTransVersionSkipIndexInfo info;
      if (OB_FAIL(reader.read_min_max_snapshot(index_row, schema_rowkey_count_, info))) {
        STORAGE_LOG(WARN, "Failed to read min max snapshot", K(ret));
      } else if (info.is_inited() && info.max_snapshot_ <= base_version_) {
        is_filtered = true;
      }
    }

    return ret;
  }

  ObSSTable &sstable_;
  const ObDatumRange &range_;
  int64_t base_version_;
  int64_t schema_rowkey_count_;
  ObEstimatedResult &result_;
  int64_t upper_trans_version_;
  bool is_ended_;

  VIRTUAL_TO_STRING_KV(K_(range),
                       K_(base_version),
                       K_(schema_rowkey_count),
                       K_(result),
                       K_(upper_trans_version),
                       K_(is_ended));
};

int ObIndexBlockScanEstimator::cal_total_estimate_result(
    ObSSTable &sstable,
    const blocksstable::ObDatumRange &datum_range,
    ObEstimatedResult &result)
{
  int ret = OB_SUCCESS;

  // TODO(menglan.sm): estimate row interface can reuse partition range spliter class.
  //                   we can estimate all range in once.
  ObIndexBlockTreeTraverser traverser;
  EstimateRowContext context(sstable, datum_range, context_.base_version_, context_.index_read_info_.get_schema_rowkey_count(), result);

  if (OB_FAIL(context.init_trans_version_if_inc_major())) {
    STORAGE_LOG(WARN, "Failed to init trans version if inc major", K(ret));
  } else if (OB_FAIL(traverser.init(sstable, context_.index_read_info_, context_.tablet_handle_.get_obj(), "OB_STORAGE_EST"))) {
    STORAGE_LOG(WARN, "Failed to init index block tree traverser", K(ret), K(context), K(traverser));
  } else if (OB_FAIL(traverser.traverse(context))) {
    STORAGE_LOG(WARN, "Failed to traverse index block tree", K(ret), K(context));
  }

  return ret;
}

// Traverser context for inverted index row-count estimation. Sums index-row row_count over
// the query range without opening any posting data micro block
class ObInvIdxEstimateRowContext final : public ObIIndexTreeTraverserContext
{
public:
  ObInvIdxEstimateRowContext(
      const ObDatumRange &range,
      const int64_t term_col_count,
      const ObStorageDatumUtils &datum_utils,
      int64_t &row_count)
    : ObIIndexTreeTraverserContext(true /*is_never_estimate*/, false /*consider_multi_version*/),
      range_(range),
      term_col_count_(term_col_count),
      term_end_key_(const_cast<ObStorageDatum *>(range.get_end_key().get_datum_ptr()), term_col_count),
      datum_utils_(datum_utils),
      row_count_(row_count),
      is_ended_(false) {}
  ~ObInvIdxEstimateRowContext() override = default;

  bool is_valid() const override { return true; }
  bool is_ended() const override { return is_ended_; }
  const ObDatumRange &get_curr_range() const override { return range_; }
  int next_range() override { is_ended_ = true; return OB_SUCCESS; }

  int on_inner_node(const ObMicroIndexInfo &index_row,
                    const bool is_coverd_by_range,
                    ObTraverserOperationType &operation) override
  {
    if (is_coverd_by_range) {
      // whole subtree / data block covered: accumulate its doc count, no descent
      row_count_ += index_row.get_row_count();
      operation = NOTHING;
    } else {
      // boundary: descend the index tree (a boundary data block is routed to on_node_estimate)
      operation = GOTO_NEXT_LEVEL;
    }
    return OB_SUCCESS;
  }

  int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range) override
  {
    int ret = OB_SUCCESS;
    if (is_coverd_by_range) {
      // interior / fully-covered leaf posting block: count whole (no posting data IO)
      row_count_ += index_row.get_row_count();
    } else if (!range_.is_right_closed()) {
      // exclusive right boundary: keep counting (no term-prefix check)
      row_count_ += index_row.get_row_count();
    } else {
      // inclusive right boundary: count the block only if its endkey term prefix matches the
      // range endkey term prefix
      int cmp_ret = 0;
      if (OB_FAIL(index_row.endkey_.compare(term_end_key_, datum_utils_, cmp_ret, false /*compare_datum_cnt*/))) {
        STORAGE_LOG(WARN, "fail to compare inv endkey term prefix", K(ret), KPC(this));
      } else if (0 == cmp_ret) {
        row_count_ += index_row.get_row_count();
      }
    }
    return ret;
  }

  int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est) override
  {
    UNUSEDx(index_row, est);
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "inv sstable estimation must not open data blocks", K(ret));
    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(range), K_(term_col_count), K_(term_end_key), K_(row_count), K_(is_ended));

private:
  const ObDatumRange &range_;
  const int64_t term_col_count_;
  ObDatumRowkey term_end_key_;
  const ObStorageDatumUtils &datum_utils_;
  int64_t &row_count_;
  bool is_ended_;
};

int ObIndexBlockScanEstimator::estimate_inv_sstable_row_count(
    ObSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObDatumRange &range,
    ObTablet *tablet,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  row_count = 0;
  if (OB_UNLIKELY(!read_info.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument for inv sstable estimate row count", K(ret), K(range));
  } else if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inv sstable", K(ret));
  } else if (sstable.is_empty()) {
    // empty segment: 0 rows
  } else {
    // Inverted index rowkey is (term..., doc_id): the term is the leading (schema_rowkey_cnt - 1)
    // columns. Only support term range scan for now.
    const int64_t schema_rowkey_cnt = read_info.get_schema_rowkey_count();
    const int64_t term_col_count = schema_rowkey_cnt - 1;
    if (OB_UNLIKELY(term_col_count < 1)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "inv sstable estimate only support inverted term range", K(ret), K(schema_rowkey_cnt));
    } else if (OB_UNLIKELY(range.get_start_key().get_datum_cnt() < term_col_count
                         || range.get_end_key().get_datum_cnt() < term_col_count)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "inv sstable estimate only support inverted term range",
          K(ret), K(term_col_count), K(schema_rowkey_cnt), K(range));
    } else {
      bool is_term_only_range = true;
      const int64_t start_datum_cnt = range.get_start_key().get_datum_cnt();
      const int64_t end_datum_cnt = range.get_end_key().get_datum_cnt();
      for (int64_t idx = term_col_count; is_term_only_range && idx < start_datum_cnt; ++idx) {
        if (!range.get_start_key().get_datum(idx).is_min()) {
          is_term_only_range = false;
        }
      }
      for (int64_t idx = term_col_count; is_term_only_range && idx < end_datum_cnt; ++idx) {
        if (!range.get_end_key().get_datum(idx).is_max()) {
          is_term_only_range = false;
        }
      }
      if (OB_UNLIKELY(!is_term_only_range)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "inv sstable estimate only supports inverted term range (tail datums must be MIN/MAX)",
            K(ret), K(term_col_count), K(schema_rowkey_cnt), K(range));
      } else {
        ObIndexBlockTreeTraverser traverser;
        ObInvIdxEstimateRowContext context(range, term_col_count, read_info.get_datum_utils(), row_count);
        if (OB_FAIL(traverser.init(sstable, read_info, tablet, "OB_INV_EST"))) {
          STORAGE_LOG(WARN, "failed to init index block tree traverser for inv sstable", K(ret));
        } else if (OB_FAIL(traverser.traverse(context))) {
          STORAGE_LOG(WARN, "failed to traverse inv sstable index tree", K(ret), K(range));
        }
      }
    }
  }
  return ret;
}
// Traverser context for async IVF index row count / block count estimation. The low precision
// vector data micro blocks of an IVF sstable may be FastScan encoded and carry no row-format micro
// header, so they must never be opened for estimation; this context only accumulates what the index
// rows already carry: an index row aggregates the row count of its whole subtree, and a leaf data
// index row stands for exactly one data micro block.
class ObIvfIdxEstimateRowContext final : public ObIIndexTreeTraverserContext
{
public:
  ObIvfIdxEstimateRowContext(const ObDatumRange &range, ObIvfEstimateResult &result)
      : ObIIndexTreeTraverserContext(true /*is_never_estimate*/, false /*consider_multi_version*/),
        range_(range),
        result_(result),
        is_ended_(false)
  {
    result_.row_count_ = 0;
    result_.macro_block_cnt_ = 0;
    result_.micro_block_cnt_ = 0;
  }
  ~ObIvfIdxEstimateRowContext() override = default;

  bool is_valid() const override { return true; }
  bool is_ended() const override { return is_ended_; }
  const ObDatumRange &get_curr_range() const override { return range_; }
  int next_range() override { is_ended_ = true; return OB_SUCCESS; }

  int on_inner_node(const ObMicroIndexInfo &index_row,
                    const bool is_coverd_by_range,
                    ObTraverserOperationType &operation) override
  {
    if (is_coverd_by_range) {
      // whole subtree / data block covered: accumulate it, no descent (no data micro block IO)
      accumulate(index_row);
      operation = NOTHING;
    } else {
      // boundary: descend the index tree (a boundary data block is routed to on_node_estimate)
      operation = GOTO_NEXT_LEVEL;
    }
    return OB_SUCCESS;
  }

  int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range) override
  {
    // Reached for a data block intersecting the range (the subtrees out of the range have already
    // been pruned by locate_range). Opening the block is forbidden, so the whole block is counted
    // conservatively, i.e. the estimation is a upper bound of the real row count.
    UNUSED(is_coverd_by_range);
    accumulate(index_row);
    return OB_SUCCESS;
  }

  int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est) override
  {
    UNUSED(est);
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ivf sstable estimation must not open data blocks", K(ret), K(index_row));
    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(range), K_(result), K_(is_ended));

private:
  void accumulate(const ObMicroIndexInfo &index_row)
  {
    if (index_row.is_data_block()) {
      // one data index row = one data micro block in one macro block. The block level counts of a
      // leaf data index row may not be filled by the index builder, so floor them to 1.
      result_.macro_block_cnt_ += 1;
      result_.micro_block_cnt_ += 1;
    } else {
      const int64_t macro_block_cnt = static_cast<int64_t>(index_row.get_macro_block_count());
      const int64_t micro_block_cnt = static_cast<int64_t>(index_row.get_micro_block_count());
      result_.macro_block_cnt_ += MAX(macro_block_cnt, 1);
      result_.micro_block_cnt_ += MAX(micro_block_cnt, 1);
    }
    result_.row_count_ += static_cast<int64_t>(index_row.get_row_count());
  }

private:
  const ObDatumRange &range_;
  ObIvfEstimateResult &result_;
  bool is_ended_;
};

int ObIndexBlockScanEstimator::estimate_ivf_sstable_result(
    ObSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObDatumRange &range,
    ObTablet *tablet,
    ObIvfEstimateResult &result)
{
  int ret = OB_SUCCESS;
  result.row_count_ = 0;
  result.macro_block_cnt_ = 0;
  result.micro_block_cnt_ = 0;
  if (OB_UNLIKELY(!read_info.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument for ivf sstable estimation", K(ret), K(range));
  } else if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ivf sstable", K(ret));
  } else if (sstable.is_empty()) {
    // empty segment (e.g. an empty refreshed table or an empty incremental segment): 0 rows
  } else {
    ObIndexBlockTreeTraverser traverser;
    ObIvfIdxEstimateRowContext context(range, result);
    if (OB_FAIL(traverser.init(sstable, read_info, tablet, "OB_IVF_EST"))) {
      STORAGE_LOG(WARN, "failed to init index block tree traverser for ivf sstable", K(ret));
    } else if (OB_FAIL(traverser.traverse(context))) {
      STORAGE_LOG(WARN, "failed to traverse ivf sstable index tree", K(ret), K(range));
    }
  }
  return ret;
}

int ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
    ObSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObDatumRange &range,
    ObTablet *tablet,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  ObIvfEstimateResult result;
  row_count = 0;
  if (OB_FAIL(estimate_ivf_sstable_result(sstable, read_info, range, tablet, result))) {
    STORAGE_LOG(WARN, "failed to estimate ivf sstable result", K(ret));
  } else {
    row_count = result.row_count_;
  }
  return ret;
}


}
}

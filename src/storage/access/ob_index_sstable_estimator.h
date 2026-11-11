/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_INDEX_SSTABLE_ESTIMATOR_H
#define OCEANBASE_STORAGE_OB_INDEX_SSTABLE_ESTIMATOR_H

#include "storage/access/ob_micro_block_handle_mgr.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}
namespace storage
{
class ObITableReadInfo;
class ObTablet;
struct ObPartitionEst
{
  int64_t logical_row_count_;
  int64_t physical_row_count_;
  OB_INLINE bool is_invalid_memtable_result() const { return logical_row_count_ <= 0 && physical_row_count_ > 1024; }
  TO_STRING_KV(K_(logical_row_count), K_(physical_row_count));

  ObPartitionEst();
  int add(const ObPartitionEst &pe);
  int deep_copy(const ObPartitionEst &src);
  void reset() { logical_row_count_ = physical_row_count_ = 0; }
  bool operator ==(const ObPartitionEst &other) const { return logical_row_count_ == other.logical_row_count_ &&
                                                               physical_row_count_ == other.physical_row_count_; }
};

struct ObIndexSSTableEstimateContext
{
public:
  ObIndexSSTableEstimateContext(
      const ObTabletHandle &tablet_handle,
      const common::ObQueryFlag &query_flag,
      const int64_t base_version = -1)
      : index_read_info_(tablet_handle.get_obj()->get_rowkey_read_info()),
      tablet_handle_(tablet_handle),
      query_flag_(query_flag),
      base_version_(base_version)
  {
  }

  ~ObIndexSSTableEstimateContext() = default;

  TO_STRING_KV(K_(tablet_handle), K_(query_flag));
  const ObITableReadInfo &index_read_info_;
  const ObTabletHandle &tablet_handle_;
  const common::ObQueryFlag &query_flag_;
  const int64_t base_version_;
};

struct ObEstimatedResult
{
  ObEstimatedResult(const bool only_block = false)
    : total_row_count_(0), total_row_count_delta_(0), macro_block_cnt_(0), micro_block_cnt_(0),
    only_block_(only_block)
  {
  }
  int64_t total_row_count_;
  int64_t total_row_count_delta_;
  uint64_t macro_block_cnt_;
  uint64_t micro_block_cnt_;
  bool only_block_;
  TO_STRING_KV(K_(total_row_count), K_(total_row_count_delta), K_(macro_block_cnt), K_(micro_block_cnt), K_(only_block));
};

// Estimation result of an async IVF index sstable. It is produced by traversing only the index
// block tree, so the counts come from the index rows instead of opened data micro blocks.
struct ObIvfEstimateResult
{
  ObIvfEstimateResult() : row_count_(0), macro_block_cnt_(0), micro_block_cnt_(0) {}
  TO_STRING_KV(K_(row_count), K_(macro_block_cnt), K_(micro_block_cnt));
  int64_t row_count_;
  int64_t macro_block_cnt_;
  int64_t micro_block_cnt_;
};

class ObIndexBlockScanEstimator
{
public:
  ObIndexBlockScanEstimator(const ObIndexSSTableEstimateContext &context);
  ~ObIndexBlockScanEstimator() = default;
  void reuse() {}
  int estimate_row_count(blocksstable::ObSSTable &sstable,
                         const blocksstable::ObDatumRange &datum_range,
                         ObPartitionEst &part_est);
  int estimate_block_count(blocksstable::ObSSTable &sstable,
                           const blocksstable::ObDatumRange &datum_range,
                           int64_t &macro_block_cnt,
                           int64_t &micro_block_cnt);
  // Estimate the number of (term, doc_id) pairs of an inverted index sstable in the given
  // range. Traverses only the inverted index block tree (intermediate index levels down to
  // leaf index rows); it does NOT open any posting data micro block.
  static int estimate_inv_sstable_row_count(
      blocksstable::ObSSTable &sstable,
      const ObITableReadInfo &read_info,
      const blocksstable::ObDatumRange &range,
      ObTablet *tablet,
      int64_t &row_count);
  // Estimate the row count and block counts of an async IVF index sstable (baseline
  // IVF_INDEX_SSTABLE and incremental INC_IVF_INDEX_SSTABLE) in the given range. Only the IVF
  // index block tree is traversed: the low-precision vector data micro blocks may be FastScan
  // encoded, holding no row-format micro header (see ObMicroBlockData::get_micro_header()), so
  // they must never be opened here. The row/block counts of an index row are aggregated into
  // its subtree, and a leaf data index row stands for exactly one data micro block.
  static int estimate_ivf_sstable_result(
      blocksstable::ObSSTable &sstable,
      const ObITableReadInfo &read_info,
      const blocksstable::ObDatumRange &range,
      ObTablet *tablet,
      ObIvfEstimateResult &result);
  static int estimate_ivf_sstable_row_count(
      blocksstable::ObSSTable &sstable,
      const ObITableReadInfo &read_info,
      const blocksstable::ObDatumRange &range,
      ObTablet *tablet,
      int64_t &row_count);
private:
  int cal_total_estimate_result(blocksstable::ObSSTable &sstable,
      const blocksstable::ObDatumRange &datum_range,
      ObEstimatedResult &result);
  int estimate_data_block_row_count(
      const blocksstable::ObDatumRange &range,
      ObMicroBlockDataHandle &micro_handle,
      bool consider_multi_version,
       ObPartitionEst &est);
  uint64_t tenant_id_;
  const ObIndexSSTableEstimateContext &context_;
};

}
}
#endif /* OCEANBASE_STORAGE_OB_INDEX_SSTABLE_ESTIMATOR_H */

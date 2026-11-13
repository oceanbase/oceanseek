/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGGREGATOR_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGGREGATOR_H_

#include "ob_inv_idx_agg_info.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObIndexBlockRowDesc;
class ObInvMacroMeta;

// Owns BM25 stats while the common index builder only dispatches rows.
class ObInvIdxAggregator final
{
public:
  ObInvIdxAggregator() : agg_info_(), has_rows_(false) { reset(); }
  void reset();
  bool has_rows() const { return has_rows_; }
  int eval(const ObIndexBlockRowDesc &row_desc);
  int fill_row_desc(ObIndexBlockRowDesc &row_desc, common::ObIAllocator &allocator) const;
  // The caller owns the encoded bytes and must not reset/reuse allocator until
  // row_desc has been written and evaluated by its parent aggregator.
  static int fill_row_desc(const ObInvIdxAggInfo &summary, ObIndexBlockRowDesc &row_desc,
                           common::ObIAllocator &allocator);
  static int get_summary_from_macro(const ObInvMacroMeta &macro_meta, ObInvIdxAggInfo &summary);
  TO_STRING_KV(K_(agg_info), K_(has_rows));
private:
  ObInvIdxAggInfo agg_info_;
  bool has_rows_;
};

} // namespace blocksstable
} // namespace oceanbase
#endif

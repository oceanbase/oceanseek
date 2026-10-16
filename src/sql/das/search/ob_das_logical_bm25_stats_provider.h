/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_LOGICAL_BM25_STATS_PROVIDER_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_LOGICAL_BM25_STATS_PROVIDER_H_

#include "sql/das/search/ob_das_logical_bm25_stats.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace sql
{

class ObILogicalBm25ScanHost;
struct ObDASBaseCtDef;
struct ObDASBaseRtDef;
struct ObDASScalarScanCtDef;
struct ObDASScalarScanRtDef;
struct ObDASScanCtDef;
struct ObDASScanRtDef;

/// Query-scope provider for BM25 stats over the pyseekdb namespace ES filter set.
/// Lazy three-pass design:
///   pass 1: build filtered_doc_set_ from logical primary scan + filter eval
///   pass 2: scan forward aux for (rowid, doc_length); accumulate doc_len_sum_
///   pass 3 (per token): scan inverted aux range for token; count membership in filtered_doc_set_
/// First version supports single physical KV tablet only; multi-tablet returns OB_NOT_SUPPORTED.
///
/// The provider is host-agnostic: it works with either ObDASScalarScanCtDef (hybrid
/// search via ObDASSearchCtx) or ObDASScanCtDef (plain logical-table MATCH via
/// ObDASTRMergeIter). The `use_scalar_ctdef_` flag picks the correct cast when
/// accessing fields that live on the concrete subclass.
class ObLogicalFilteredBM25StatsProvider
{
public:
  ObLogicalFilteredBM25StatsProvider();
  ~ObLogicalFilteredBM25StatsProvider() { reset(); }
  int init(const ObLogicalFilteredBM25StatsKey &key,
           const ObDASBaseCtDef *primary_stats_ctdef,
           ObDASBaseRtDef *primary_stats_rtdef,
           const ObDASBaseCtDef *inv_scan_ctdef,
           ObDASBaseRtDef *inv_scan_rtdef,
           const sql::ExprFixedArray *filter_exprs,
           bool use_scalar_ctdef,
           ObILogicalBm25ScanHost *host);
  void reset();
  bool is_inited() const { return is_inited_; }

  /// Lazily ensures filtered_doc_set_ and doc_length stats are built, then returns
  /// total_doc_cnt and avg_doc_token_cnt.
  int get_total_doc_cnt_and_avg_doc_len(ObLogicalFilteredBM25Stats &stats);
  /// Lazily scans the inverted range for `token` and returns the count of postings
  /// whose rowid lies in filtered_doc_set_. Result is cached per-token.
  int get_token_doc_cnt(const ObString &token, int64_t &token_doc_cnt);
  const ObLogicalFilteredBM25StatsKey &get_key() const { return key_; }

  TO_STRING_KV(K_(key), K_(stats), K_(filtered_doc_set_built), K_(doc_length_stats_built),
               K_(use_scalar_ctdef));

private:
  int build_filtered_doc_set_once_();
  int build_doc_length_stats_once_();
  int scan_inverted_token_filtered_(const ObString &token, int64_t &token_doc_cnt);

  int init_and_open_scalar_iter_(
      const ObDASBaseCtDef *ctdef,
      ObDASBaseRtDef *rtdef,
      storage::ObTableScanParam &scan_param,
      ObDASScanIter &iter,
      common::ObTabletID &tablet_id,
      bool &inited);
  int release_scalar_iter_(
      storage::ObTableScanParam &scan_param,
      ObDASScanIter &iter,
      bool &inited);

  // Field accessors that dispatch on use_scalar_ctdef_. Both ObDASScalarScanCtDef
  // and ObDASScanCtDef carry these fields with identical types but live on
  // distinct base classes — only the static_cast target differs.
  common::ObTableID get_ref_table_id_(const ObDASBaseCtDef *ctdef) const;
  const sql::ExprFixedArray &get_rowkey_exprs_(const ObDASBaseCtDef *ctdef) const;
  const sql::ExprFixedArray &get_logical_output_exprs_(const ObDASBaseCtDef *ctdef) const;
  const sql::ExprFixedArray &get_result_output_(const ObDASBaseCtDef *ctdef) const;

private:
  ObLogicalFilteredBM25StatsKey key_;
  const ObDASBaseCtDef *primary_stats_ctdef_;
  ObDASBaseRtDef *primary_stats_rtdef_;
  const ObDASBaseCtDef *inv_scan_ctdef_;
  ObDASBaseRtDef *inv_scan_rtdef_;
  const sql::ExprFixedArray *filter_exprs_;
  ObILogicalBm25ScanHost *host_;
  ObLogicalFilteredBM25Stats stats_;
  common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> filtered_doc_ids_;
  common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> doc_len_seen_ids_;
  storage::ObTableScanParam primary_scan_param_;
  ObDASScanIter primary_scan_iter_;
  common::ObTabletID primary_tablet_id_;
  bool primary_scan_inited_;
  storage::ObTableScanParam inv_scan_param_;
  ObDASScanIter inv_scan_iter_;
  common::ObTabletID inv_tablet_id_;
  bool inv_scan_inited_;
  common::ObObj doc_len_range_objs_[4];
  common::ObObj token_range_objs_[4];
  bool filtered_doc_set_built_;
  bool doc_length_stats_built_;
  bool use_scalar_ctdef_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_LOGICAL_BM25_STATS_PROVIDER_H_

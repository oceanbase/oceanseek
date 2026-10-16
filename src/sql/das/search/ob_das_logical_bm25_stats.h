/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_LOGICAL_BM25_STATS_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_LOGICAL_BM25_STATS_H_

#include "sql/das/search/ob_das_search_define.h"

namespace oceanbase
{
namespace sql
{

struct ObDASIRScanCtDef;

/// Cache key for query-scope filtered BM25 stats.
/// Identifies a unique (logical scope, IR scan ctdef, filter expr set) combination.
/// Pointers are valid for the lifetime of the SQL plan & search ctx.
struct ObLogicalFilteredBM25StatsKey
{
  uint64_t namespace_id_;
  uint64_t logical_table_id_;
  uint64_t logical_index_seq_;
  uint64_t physical_data_table_id_;
  common::ObTabletID physical_tablet_id_;
  const sql::ObDASIRScanCtDef *ir_ctdef_;
  const sql::ExprFixedArray *filter_exprs_;
  ObLogicalFilteredBM25StatsKey()
    : namespace_id_(0), logical_table_id_(0), logical_index_seq_(0),
      physical_data_table_id_(common::OB_INVALID_ID), physical_tablet_id_(),
      ir_ctdef_(nullptr), filter_exprs_(nullptr) {}
  bool operator==(const ObLogicalFilteredBM25StatsKey &other) const
  {
    return namespace_id_ == other.namespace_id_
        && logical_table_id_ == other.logical_table_id_
        && logical_index_seq_ == other.logical_index_seq_
        && physical_data_table_id_ == other.physical_data_table_id_
        && physical_tablet_id_ == other.physical_tablet_id_
        && ir_ctdef_ == other.ir_ctdef_
        && filter_exprs_ == other.filter_exprs_;
  }
  TO_STRING_KV(K_(namespace_id), K_(logical_table_id), K_(logical_index_seq),
               K_(physical_data_table_id), K_(physical_tablet_id),
               KP_(ir_ctdef), KP_(filter_exprs));
};

/// Aggregate filtered BM25 stats for one (namespace, ltable, IR ctdef) scope.
struct ObLogicalFilteredBM25Stats
{
  uint64_t total_doc_cnt_;
  double avg_doc_token_cnt_;
  ObLogicalFilteredBM25Stats() : total_doc_cnt_(0), avg_doc_token_cnt_(0.0) {}
  TO_STRING_KV(K_(total_doc_cnt), K_(avg_doc_token_cnt));
};

class ObLogicalFilteredBM25StatsProvider;

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_LOGICAL_BM25_STATS_H_

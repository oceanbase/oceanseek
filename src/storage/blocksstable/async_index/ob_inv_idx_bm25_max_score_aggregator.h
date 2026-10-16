/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_BM25_MAX_SCORE_AGGREGATOR_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_BM25_MAX_SCORE_AGGREGATOR_H_

#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObInvIdxBM25ParamPair
{
  uint32_t token_freq_;
  uint32_t doc_length_;
  TO_STRING_KV(K_(token_freq), K_(doc_length));
};

// Aggregate the Pareto frontier of BM25 params for one posting block.
// Each pair keeps a token_freq / doc_length combination that may produce the
// block max score under different avg doc length values.
class ObInvIdxBM25MaxScoreAggregator final
{
public:
  ObInvIdxBM25MaxScoreAggregator();
  ~ObInvIdxBM25MaxScoreAggregator();

  int init(ObIAllocator &allocator, const int64_t max_pair_count);

  // The output array is owned by this aggregator and remains valid until the next aggregate(),
  // reuse(), or reset().
  int aggregate(
      const ObIArray<uint64_t> &token_freqs,
      const ObIArray<uint64_t> &doc_lengths,
      const ObIArray<ObInvIdxBM25ParamPair> *&frontier_pairs);

  void reuse();
  void reset();

  TO_STRING_KV(K_(is_inited), K_(max_pair_count),
      "frontier_pair_count", frontier_pairs_.count());

private:
  ObIAllocator *allocator_;
  int64_t max_pair_count_;
  ObFixedArray<ObInvIdxBM25ParamPair, ObIAllocator> frontier_pairs_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObInvIdxBM25MaxScoreAggregator);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_BM25_MAX_SCORE_AGGREGATOR_H_

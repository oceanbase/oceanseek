/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_BM25_H_
#define _OB_EXPR_BM25_H_

#include <cmath>
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

// Query/segment-level BM25 constants precomputed once per scan so the per-document
// hot path only does a single floating-point division. Filled exclusively by
// ObExprBM25::init_precomputed_params(). See ob_expr_bm25.cpp for the formula.
struct ObBM25PrecomputedParams
{
  ObBM25PrecomputedParams() { reset(); }

  void reset()
  {
    query_weight_ = 0.0;
    score_multiplier_ = 0.0;
    denom_bias_ = 0.0;
    doc_len_scale_ = 0.0;
    is_inited_ = false;
  }

  TO_STRING_KV(K_(query_weight), K_(score_multiplier), K_(denom_bias), K_(doc_len_scale), K_(is_inited));

  double query_weight_;
  double score_multiplier_;
  double denom_bias_;
  double doc_len_scale_;
  bool is_inited_;
};

/**
 * An implementation of Okapi BM25 relevance estimation / ranking algorithm
 *
 * Params:
 *    token document count: count of documents contains query token
 *    total document count: count of all documents in retrieval domain
 *    document token count: count of tokens in specific document
 *    average document token count: average count of tokens in document in retrieval domain
 *    related token count: count of query token in specific document
 *  
 *    p_k1, p_b, p_epsilon: parameters to tune bm25 score, hard coded for now.
 */
class ObExprBM25 : public ObFuncExprOperator
{
public:
  explicit ObExprBM25(common::ObIAllocator &alloc);
  virtual ~ObExprBM25() {}

  virtual int calc_result_typeN(
      ObExprResType &result_type,
      ObExprResType *types,
      int64_t param_num,
      common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx,
      const ObRawExpr &raw_expr,
      ObExpr &rt_expr) const override;

  static int eval_bm25_relevance_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int eval_batch_bm25_relevance_expr(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t size);
  static double eval(
      const int64_t token_freq,
      const int64_t doc_length,
      const int64_t doc_freq,
      const int64_t doc_cnt,
      const double avg_doc_token_cnt)
  {
    OB_ASSERT(avg_doc_token_cnt != 0);
    const double norm_len = doc_length / avg_doc_token_cnt;
    const double score = query_token_weight(doc_freq, doc_cnt) * doc_token_weight(token_freq, norm_len);
    return score;
  }
  static double length_normalizer(const int64_t dl, const double avg_dl);
  static double doc_token_weight(const int64_t token_freq, const double norm_len)
  {
    const double tf = static_cast<double>(token_freq);
    return tf / (tf + p_k1 * (1.0 - p_b + p_b * norm_len));
  }
  // Precompute the query/segment-level constants that do not vary per document, so the
  // scoring hot path collapses to a single division. Fails with OB_INVALID_ARGUMENT on
  // avg_doc_length <= 0 or any non-finite input; on success sets params.is_inited_ = true.
  static int init_precomputed_params(
      const double query_token_weight,
      const double avg_doc_length,
      const double score_multiplier,
      ObBM25PrecomputedParams &params);

  // Scalar fast path: score one document with a single division. Keeps the
  // query_weight * tf_score * score_multiplier multiplication order so the row score and
  // the block-max upper bound share the same floating-point rounding. This is the single
  // shared kernel used by scalar, batch, and block-max paths.
  static OB_INLINE double eval_with_precomputed_params(
      const uint64_t token_freq,
      const uint64_t doc_length,
      const ObBM25PrecomputedParams &params)
  {
    OB_ASSERT(params.is_inited_);
    const double tf = static_cast<double>(token_freq);
    const double denom = tf + params.denom_bias_ + params.doc_len_scale_ * static_cast<double>(doc_length);
    return params.query_weight_ * (tf / denom) * params.score_multiplier_;
  }

  // Batch fast path: contiguous SoA inputs -> contiguous scores. Delegates to the scalar
  // kernel so batch and scalar results are bit-identical.
  //
  // __restrict contract: over [0, count) the three arrays must not overlap, and during
  // execution their elements must not be accessed or mutated through any other alias.
  // Violating this is undefined behavior (restrict is a compile-time optimization hint,
  // not a runtime check). Current call sites satisfy it: token_freqs / doc_lengths are two
  // read-only columns of a posting block, scores is a distinct downstream double buffer.
  static void eval_batch_with_precomputed_params(
      const uint64_t *__restrict token_freqs,
      const uint64_t *__restrict doc_lengths,
      const int64_t count,
      const ObBM25PrecomputedParams &params,
      double *__restrict scores)
  {
    OB_ASSERT(count >= 0);
    OB_ASSERT(count <= 0 || (nullptr != token_freqs && nullptr != doc_lengths && nullptr != scores));
    for (int64_t i = 0; i < count; ++i) {
      scores[i] = eval_with_precomputed_params(token_freqs[i], doc_lengths[i], params);
    }
  }
  static double doc_phrase_weight(const double phrase_freq, const double norm_len);
  static double query_token_weight(const int64_t doc_freq, const int64_t doc_cnt);
  static double max_term_bm25_score(const int64_t doc_freq, const int64_t doc_cnt)
  {
    return query_token_weight(doc_freq, doc_cnt);
  }
  static bool use_new_version(const ObExpr &expr)
  {
    return 6 == expr.arg_cnt_;
  }
public:
  static constexpr int TOKEN_DOC_CNT_PARAM_IDX = 0;
  static constexpr int TOTAL_DOC_CNT_PARAM_IDX = 1;

  // Old version (5 params, < CLUSTER_VERSION_4_5_1_0)
  static constexpr int DOC_TOKEN_CNT_PARAM_IDX = 2;
  static constexpr int AVG_DOC_CNT_PARAM_IDX_OLD = 3;
  static constexpr int RELATED_TOKEN_CNT_PARAM_IDX_OLD = 4;
  
  // New version (6 params, >= CLUSTER_VERSION_4_5_1_0)
  static constexpr int DOC_LENGTH_PARAM_IDX = 2;
  static constexpr int TOKEN_WEIGHT_PARAM_IDX = 3;
  static constexpr int AVG_DOC_CNT_PARAM_IDX = 4;
  static constexpr int RELATED_TOKEN_CNT_PARAM_IDX = 5;
  static constexpr double DEFAULT_AVG_DOC_TOKEN_CNT = 10.0;
private:
  static constexpr double p_k1 = 1.2;
  static constexpr double p_b = 0.75;
  static constexpr double p_epsilon = 0.25;
  DISALLOW_COPY_AND_ASSIGN(ObExprBM25);
};

} // namespace sql
} // namespace oceanbase

#endif

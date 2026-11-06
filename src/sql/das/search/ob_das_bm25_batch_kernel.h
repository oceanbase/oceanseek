/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef SQL_DAS_SEARCH_OB_DAS_BM25_BATCH_KERNEL_H_
#define SQL_DAS_SEARCH_OB_DAS_BM25_BATCH_KERNEL_H_

#include "common/ob_target_specific.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"

namespace oceanbase
{
namespace sql
{

struct ObBM25PrecomputedParams;

template <bool ONLY_EXISTING> class BM25AccumulationKernels;

template <bool ONLY_EXISTING>
BM25AccumulationKernels<ONLY_EXISTING> get_bm25_accumulation_kernels();

// Default selects scalar. Unsupported targets return OB_NOT_SUPPORTED and clear the output.
template <bool ONLY_EXISTING>
int get_bm25_accumulation_kernels_for_arch(
    const common::ObTargetArch arch, BM25AccumulationKernels<ONLY_EXISTING> &kernels);

// Only the factories can bind a mode to its four physical column-width implementations.
template <bool ONLY_EXISTING>
class BM25AccumulationKernels final
{
public:
  BM25AccumulationKernels()
    : doc32_rel32_(nullptr), doc32_rel64_(nullptr), doc64_rel32_(nullptr), doc64_rel64_(nullptr),
      arch_(common::ObTargetArch::Default) {}

  bool is_valid() const
  {
    return nullptr != doc32_rel32_ && nullptr != doc32_rel64_
        && nullptr != doc64_rel32_ && nullptr != doc64_rel64_;
  }
  common::ObTargetArch get_arch() const { return arch_; }

private:
  friend class DocScoreAccumulator;
  friend BM25AccumulationKernels<ONLY_EXISTING> get_bm25_accumulation_kernels<ONLY_EXISTING>();
  friend int get_bm25_accumulation_kernels_for_arch<ONLY_EXISTING>(
      const common::ObTargetArch arch, BM25AccumulationKernels<ONLY_EXISTING> &kernels);

  using Kernel = int64_t (*)(DocScoreAccumulator &, const void *, const void *, const void *,
      const int64_t, const ObBM25PrecomputedParams &);

  BM25AccumulationKernels(const Kernel doc32_rel32, const Kernel doc32_rel64,
      const Kernel doc64_rel32, const Kernel doc64_rel64, const common::ObTargetArch arch)
    : doc32_rel32_(doc32_rel32), doc32_rel64_(doc32_rel64), doc64_rel32_(doc64_rel32),
      doc64_rel64_(doc64_rel64), arch_(arch) {}

  static BM25AccumulationKernels make(const common::ObTargetArch arch);
  Kernel select(const uint32_t doc_id_width, const uint32_t relevance_width) const
  {
    return sizeof(uint32_t) == doc_id_width
        ? (sizeof(uint32_t) == relevance_width ? doc32_rel32_ : doc32_rel64_)
        : (sizeof(uint32_t) == relevance_width ? doc64_rel32_ : doc64_rel64_);
  }

  Kernel doc32_rel32_;
  Kernel doc32_rel64_;
  Kernel doc64_rel32_;
  Kernel doc64_rel64_;
  common::ObTargetArch arch_;
};

extern template BM25AccumulationKernels<false> get_bm25_accumulation_kernels<false>();
extern template BM25AccumulationKernels<true> get_bm25_accumulation_kernels<true>();
extern template int get_bm25_accumulation_kernels_for_arch<false>(
    const common::ObTargetArch arch, BM25AccumulationKernels<false> &kernels);
extern template int get_bm25_accumulation_kernels_for_arch<true>(
    const common::ObTargetArch arch, BM25AccumulationKernels<true> &kernels);

class BM25ScoreKernels;
BM25ScoreKernels get_bm25_score_kernels();
// Default selects scalar. Unsupported targets return OB_NOT_SUPPORTED and clear the output.
int get_bm25_score_kernels_for_arch(const common::ObTargetArch arch, BM25ScoreKernels &kernels);

// CPU dispatch is shared by both widths; each batch selects its physical relevance columns.
class BM25ScoreKernels final
{
public:
  BM25ScoreKernels() : rel32_(nullptr), rel64_(nullptr), arch_(common::ObTargetArch::Default) {}
  bool is_valid() const { return nullptr != rel32_ && nullptr != rel64_; }
  common::ObTargetArch get_arch() const { return arch_; }

  OB_INLINE void score(const uint32_t *token_freqs, const uint32_t *doc_lengths,
      const int64_t count, const ObBM25PrecomputedParams &params, double *scores) const
  {
    OB_ASSERT(is_valid());
    OB_ASSERT(count >= 0);
    OB_ASSERT(count <= 0 || (nullptr != token_freqs && nullptr != doc_lengths && nullptr != scores));
    rel32_(token_freqs, doc_lengths, count, params, scores);
  }
  OB_INLINE void score(const uint64_t *token_freqs, const uint64_t *doc_lengths,
      const int64_t count, const ObBM25PrecomputedParams &params, double *scores) const
  {
    OB_ASSERT(is_valid());
    OB_ASSERT(count >= 0);
    OB_ASSERT(count <= 0 || (nullptr != token_freqs && nullptr != doc_lengths && nullptr != scores));
    rel64_(token_freqs, doc_lengths, count, params, scores);
  }
  OB_INLINE void score(const blocksstable::ObPostingRelevanceView &relevance,
      const int64_t count, const ObBM25PrecomputedParams &params, double *scores) const
  {
    if (relevance.is_uint32()) {
      score(relevance.get_token_freqs32(), relevance.get_doc_lengths32(), count, params, scores);
    } else {
      score(relevance.get_token_freqs64(), relevance.get_doc_lengths64(), count, params, scores);
    }
  }

private:
  friend BM25ScoreKernels get_bm25_score_kernels();
  friend int get_bm25_score_kernels_for_arch(const common::ObTargetArch arch, BM25ScoreKernels &kernels);
  using U32Kernel = void (*)(const uint32_t *, const uint32_t *,
      const int64_t, const ObBM25PrecomputedParams &, double *);
  using U64Kernel = void (*)(const uint64_t *, const uint64_t *,
      const int64_t, const ObBM25PrecomputedParams &, double *);
  BM25ScoreKernels(const U32Kernel rel32, const U64Kernel rel64, const common::ObTargetArch arch)
    : rel32_(rel32), rel64_(rel64), arch_(arch) {}
  static BM25ScoreKernels make(const common::ObTargetArch arch);

  U32Kernel rel32_;
  U64Kernel rel64_;
  common::ObTargetArch arch_;
};

class BM25FrontierMaxKernels;
BM25FrontierMaxKernels get_bm25_frontier_max_kernels();
// Default selects scalar. Unsupported targets return OB_NOT_SUPPORTED and clear the output.
int get_bm25_frontier_max_kernels_for_arch(
    const common::ObTargetArch arch, BM25FrontierMaxKernels &kernels);

// Reduce compact U32/U32 frontier pairs without materializing intermediate scores.
class BM25FrontierMaxKernels final
{
public:
  BM25FrontierMaxKernels() : kernel_(nullptr), arch_(common::ObTargetArch::Default) {}
  bool is_valid() const { return nullptr != kernel_; }
  common::ObTargetArch get_arch() const { return arch_; }

  OB_INLINE double max_score(const blocksstable::ObInvIdxBM25ParamPair *pairs, const int64_t count,
      const ObBM25PrecomputedParams &params, const double maximum) const
  {
    OB_ASSERT(is_valid());
    OB_ASSERT(count >= 0);
    OB_ASSERT(count <= 0 || nullptr != pairs);
    return kernel_(pairs, count, params, maximum);
  }

private:
  friend BM25FrontierMaxKernels get_bm25_frontier_max_kernels();
  friend int get_bm25_frontier_max_kernels_for_arch(
      const common::ObTargetArch arch, BM25FrontierMaxKernels &kernels);
  using Kernel = double (*)(const blocksstable::ObInvIdxBM25ParamPair *, const int64_t,
      const ObBM25PrecomputedParams &, const double);
  BM25FrontierMaxKernels(const Kernel kernel, const common::ObTargetArch arch)
    : kernel_(kernel), arch_(arch) {}
  static BM25FrontierMaxKernels make(const common::ObTargetArch arch);

  Kernel kernel_;
  common::ObTargetArch arch_;
};

} // namespace sql
} // namespace oceanbase

#endif // SQL_DAS_SEARCH_OB_DAS_BM25_BATCH_KERNEL_H_

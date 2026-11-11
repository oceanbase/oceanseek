/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SEARCH_BUDGET_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SEARCH_BUDGET_H_

#include <algorithm>
#include <cmath>
#include <cstdint>

namespace oceanbase
{
namespace blocksstable
{
// Each level supplies its own target count, candidate count and logical input
// population. The result counts physical children, including SOAR assignments.
inline int64_t calc_ivf_centroid_child_budget(
    const int64_t k, const int64_t num_candidates, const int64_t vector_count)
{
  int64_t budget = 0;
  if (k > 0 && num_candidates >= k && vector_count > 0) {
    const double ratio_signal = std::min(1.0, std::max(0.0,
        std::log(static_cast<double>(num_candidates) / k) / std::log(11.0)));
    const double k_signal = std::min(1.0,
        std::log1p(static_cast<double>(k)) / std::log(10001.0));
    const double visit_ratio = std::min(0.003 + 0.037 * (0.85 * ratio_signal + 0.15 * k_signal),
        0.045 * std::pow(1000000.0 / vector_count, 0.35));
    // visit_ratio <= 0.04, so the conversion remains within int64_t even for
    // a maximal vector_count. The caller always advances at least one CID.
    budget = static_cast<int64_t>(2.0 * visit_ratio * vector_count);
    // ES continues until its candidate collector is populated. Preserve the
    // existing OB refinement/filter flow by reserving enough postings here;
    // each vector can have at most two SOAR copies.
    const int64_t candidate_count = std::min(num_candidates, vector_count);
    const int64_t candidate_budget = candidate_count > INT64_MAX / 2
        ? INT64_MAX : 2 * candidate_count;
    budget = std::max(budget, candidate_budget);
  }
  return budget;
}

inline int64_t calc_ivf_parent_child_budget(
    const int64_t leaf_count, const int64_t needed_leaves, const double refine_k)
{
  const int64_t target = std::max<int64_t>(0, std::min(leaf_count, needed_leaves));
  // Both levels use the resolved query/index refinement multiplier.
  // Their target counts differ. Saturate before converting to int64_t.
  const double refined_count = target > 0 ? std::ceil(target * refine_k) : 0.0;
  const int64_t candidates = refined_count >= static_cast<double>(INT64_MAX)
      ? INT64_MAX : static_cast<int64_t>(refined_count);
  return calc_ivf_centroid_child_budget(target, candidates, leaf_count);
}

} // namespace blocksstable
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SEARCH_BUDGET_H_

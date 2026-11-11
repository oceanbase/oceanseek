/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_QUERY_VECTOR_CONTEXT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_QUERY_VECTOR_CONTEXT_H_

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"

namespace oceanbase
{
namespace blocksstable
{

// Immutable query data shared by centroid routing and exact refinement. The
// prepared vector belongs to the searcher's query arena (or an external caller
// for standalone iterators), which must outlive this context and all readers.
// Table cursors, group guards and per-round scratch do not belong here.
class ObIvfQueryVectorContext final
{
public:
  ObIvfQueryVectorContext() : query_(nullptr), dim_(0), metric_(share::VIDA_L2), computer_() {}
  int init(common::ObIAllocator &allocator, const float *query, int64_t dim,
      share::ObVectorIndexDistAlgorithm metric)
  {
    int ret = common::OB_SUCCESS;
    quantization::ObQuantizationParams params = {};
    params.dim_ = dim;
    params.is_norm_ = metric == share::VIDA_COS;
    if (OB_FAIL(computer_.init(allocator, quantization::ObQuantizationType::FP32,
        metric, params, query, nullptr))) {
      STORAGE_LOG(WARN, "[DISK_BBQ] failed to initialize centroid query computer",
          K(ret), K(dim), K(metric));
    } else {
      query_ = query;
      dim_ = dim;
      metric_ = metric;
    }
    return ret;
  }
  bool is_inited() const { return query_ != nullptr; }
  // Readers borrow the already prepared vector, not the caller's original input.
  bool matches_prepared_query(const float *query, int64_t dim,
      share::ObVectorIndexDistAlgorithm metric) const
  { return is_inited() && query_ == query && dim_ == dim && metric_ == metric; }
  const float *query() const { return query_; }
  const quantization::ObQuantizationComputer &computer() const { return computer_; }
  TO_STRING_KV(KP_(query), K_(dim), K_(metric));

private:
  const float *query_;
  int64_t dim_;
  share::ObVectorIndexDistAlgorithm metric_;
  quantization::ObQuantizationComputer computer_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfQueryVectorContext);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_QUERY_VECTOR_CONTEXT_H_

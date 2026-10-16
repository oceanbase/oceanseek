/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_DAS_HNSW_LOOKUP_STREAMING_ITER_H_
#define OCEANBASE_SQL_DAS_HNSW_LOOKUP_STREAMING_ITER_H_

#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "sql/das/ob_das_attach_define.h"

namespace oceanbase
{
namespace sql
{

class ObDASHNSWScanIter;

struct ObDASHNSWLookupStreamingCtx
{
  ObDASHNSWLookupStreamingCtx()
    : need_recompute_distance_(false),
      distance_expr_(nullptr)
  {}

  void reset_runtime()
  {
    need_recompute_distance_ = false;
  }

  bool need_recompute_distance_;
  ObExpr *distance_expr_;
};

struct ObDASHNSWLookupStreamingIterParam : public ObDASLocalLookupIterParam
{
  ObDASHNSWLookupStreamingIterParam()
    : ObDASLocalLookupIterParam(),
      streaming_ctx_(nullptr),
      hnsw_scan_iter_(nullptr),
      enable_simplified_scan_(false)
  {}

  ObDASHNSWLookupStreamingCtx *streaming_ctx_;
  ObDASHNSWScanIter *hnsw_scan_iter_;
  bool enable_simplified_scan_;
};

// Run the regular main-table lookup for HNSW-family indexes and, when the runtime
// index is HNSW BQ, recompute the exact distance in place. The upper SQL sort
// remains responsible for global TopN.
class ObDASHNSWLookupStreamingIter : public ObDASLocalLookupIter
{
public:
  ObDASHNSWLookupStreamingIter();
  virtual ~ObDASHNSWLookupStreamingIter() {}

  virtual int rescan() override;

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  enum class ScanMode
  {
    UNRESOLVED,
    SIMPLIFIED,
    LOOKUP
  };

  int prepare_scan_mode(bool is_vectorized);
  void clear_distance_input_evaluated_flags(bool is_vectorized, int64_t batch_size);
  void clear_distance_evaluated_flags(bool is_vectorized, int64_t row_count);
  int recompute_distance_batch(int64_t row_count);

private:
  ObDASHNSWLookupStreamingCtx *streaming_ctx_;
  ObDASHNSWScanIter *hnsw_scan_iter_;
  bool enable_simplified_scan_;
  ScanMode scan_mode_;
  ObBitVector *skip_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_DAS_HNSW_LOOKUP_STREAMING_ITER_H_ */

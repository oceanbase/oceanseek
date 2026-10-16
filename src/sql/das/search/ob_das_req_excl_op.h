/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_REQ_EXCL_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_REQ_EXCL_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASReqExclOpParam : public ObIDASSearchOpParam
{
public:
  ObDASReqExclOpParam(ObIDASSearchOp *required, ObIDASSearchOp *excluded)
      : ObIDASSearchOpParam(DAS_SEARCH_OP_REQ_EXCL), required_(required), excluded_(excluded) {}
  ~ObDASReqExclOpParam() {}
  OB_INLINE ObIDASSearchOp *get_required() const { return required_; }
  OB_INLINE ObIDASSearchOp *get_excluded() const { return excluded_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;

private:
  ObIDASSearchOp *required_;
  ObIDASSearchOp *excluded_;
};

class ObDASReqExclOp : public ObIDASSearchOp {
public:
  ObDASReqExclOp(ObDASSearchCtx &search_ctx)
      : ObIDASSearchOp(search_ctx), required_(nullptr), excluded_(nullptr),
        batch_allocator_(common::ObMemAttr(MTL_ID(), "DASReqExclBatch")),
        batch_ids_(nullptr),
        batch_scores_(nullptr),
        use_deletion_bitmap_(false)
  { }

  bool can_enumerate_rowids() const override
  {
    return OB_NOT_NULL(required_)
        && required_->can_enumerate_rowids();
  }

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_drain_until(const ObDASRowID &boundary,
                     int64_t capacity,
                     uint64_t *out_ids,
                     double *out_scores,
                     int64_t &count,
                     ObDASRowID &next_id_after,
                     double &next_score_after) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override;
  int do_set_min_competitive_score(const double &threshold) override;
private:
  static constexpr int64_t BATCH_SIZE = 256;
  OB_INLINE bool is_valid() { return nullptr != required_ && nullptr != excluded_; }
  int check_excluded(const ObDASRowID &target_id, int &cmp);
  bool is_deleted(const uint64_t doc_id) const;
  int ensure_batch_buffer();
  int drain_deletion_bitmap_until(const ObDASRowID &boundary,
                                  int64_t capacity,
                                  uint64_t *out_ids,
                                  double *out_scores,
                                  int64_t &count,
                                  ObDASRowID &next_id_after,
                                  double &next_score_after);

private:
  ObIDASSearchOp *required_;
  ObIDASSearchOp *excluded_;
  common::ObArenaAllocator batch_allocator_;
  uint64_t *batch_ids_;
  double *batch_scores_;
  // Selected once at open from the fixed RowID and excluded-op types.
  bool use_deletion_bitmap_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_REQEXCL_OP_H_

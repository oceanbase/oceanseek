/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_bmm_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/engine/ob_exec_context.h"
#undef protected
#undef private
#include "simple_server/env/ob_simple_cluster_test_base.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{

class MockShallowMaxOp : public ObIDASSearchOp
{
public:
  MockShallowMaxOp(ObDASSearchCtx &search_ctx,
                   const double max_score,
                   const int max_target_ret)
      : ObIDASSearchOp(search_ctx),
        max_score_(max_score),
        max_target_ret_(max_target_ret),
        shallow_count_(0),
        max_target_count_(0),
        last_inclusive_(true)
  {
    is_scoring_ = true;
  }

  int64_t shallow_count() const { return shallow_count_; }
  int64_t max_target_count() const { return max_target_count_; }
  bool last_inclusive() const { return last_inclusive_; }

private:
  int do_open() override { return OB_SUCCESS; }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override { return OB_SUCCESS; }

  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override
  {
    UNUSED(target);
    UNUSED(curr_id);
    UNUSED(score);
    return OB_NOT_SUPPORTED;
  }

  int do_next_rowid(ObDASRowID &next_id, double &score) override
  {
    UNUSED(next_id);
    UNUSED(score);
    return OB_ITER_END;
  }

  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override
  {
    int ret = OB_SUCCESS;
    ++shallow_count_;
    last_inclusive_ = inclusive;
    max_score_tuple = nullptr;
    if (target.is_max()) {
      ++max_target_count_;
      ret = max_target_ret_;
    } else if (OB_UNLIKELY(!target.is_normal())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObDASRowID max_id;
      max_id.set_max();
      max_score_tuple_.set(target, max_id, max_score_);
      max_score_tuple = &max_score_tuple_;
    }
    return ret;
  }

private:
  double max_score_;
  int max_target_ret_;
  int64_t shallow_count_;
  int64_t max_target_count_;
  bool last_inclusive_;
};

struct SearchCtxSetup
{
  explicit SearchCtxSetup(ObIAllocator &allocator)
      : scan_allocator_("BMMShallowScan"),
        scan_op_(scan_allocator_),
        exec_ctx_(allocator),
        eval_ctx_(exec_ctx_),
        rowid_exprs_(allocator),
        output_(allocator),
        search_ctx_(allocator, scan_op_)
  {
    eval_ctx_.max_batch_size_ = 256;
    scan_rtdef_.eval_ctx_ = &eval_ctx_;
    scan_op_.set_scan_rtdef(&scan_rtdef_);
  }

  int init()
  {
    search_ctx_.eval_ctx_ = &eval_ctx_;
    search_ctx_.rowid_exprs_ = &rowid_exprs_;
    search_ctx_.output_ = &output_;
    search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs_;
    return OB_SUCCESS;
  }

  ObArenaAllocator scan_allocator_;
  ObDASScanOp scan_op_;
  ObDASScanRtDef scan_rtdef_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ExprFixedArray rowid_exprs_;
  ExprFixedArray output_;
  ObDASSearchCtx search_ctx_;
};

TEST(TestBMMShallow, max_boundary_is_iteration_end)
{
  ObArenaAllocator allocator("TestBMMShallow");
  SearchCtxSetup ctx_setup(allocator);
  ASSERT_EQ(OB_SUCCESS, ctx_setup.init());

  MockShallowMaxOp first_op(ctx_setup.search_ctx_, 0.2, OB_ITER_END);
  MockShallowMaxOp second_op(ctx_setup.search_ctx_, 0.3, OB_INVALID_ARGUMENT);
  ObDAATMergeIter first_iter;
  ObDAATMergeIter second_iter;
  ASSERT_EQ(OB_SUCCESS, first_iter.init(0, first_op));
  ASSERT_EQ(OB_SUCCESS, second_iter.init(1, second_op));

  ObDASBMMOp bmm_op(ctx_setup.search_ctx_);
  bmm_op.merge_iters_.set_allocator(&allocator);
  bmm_op.sorted_iter_idxes_.set_allocator(&allocator);
  ASSERT_EQ(OB_SUCCESS, bmm_op.merge_iters_.init(2));
  ASSERT_EQ(OB_SUCCESS, bmm_op.merge_iters_.prepare_allocate(2));
  ASSERT_EQ(OB_SUCCESS, bmm_op.sorted_iter_idxes_.init(2));
  ASSERT_EQ(OB_SUCCESS, bmm_op.sorted_iter_idxes_.prepare_allocate(2));
  bmm_op.merge_iters_[0] = &first_iter;
  bmm_op.merge_iters_[1] = &second_iter;
  bmm_op.sorted_iter_idxes_[0] = 0;
  bmm_op.sorted_iter_idxes_[1] = 1;
  bmm_op.min_competitive_score_ = 1.0;
  bmm_op.non_essential_dim_count_ = 0;
  bmm_op.non_essential_dim_max_score_ = 0.0;

  ObDASRowID target;
  target.set_uint64(606);
  EXPECT_EQ(OB_ITER_END, bmm_op.compute_vec_boundary_at(target));
  EXPECT_EQ(1, first_op.shallow_count());
  EXPECT_EQ(1, second_op.shallow_count());
  EXPECT_TRUE(first_op.last_inclusive());
  EXPECT_TRUE(second_op.last_inclusive());
  EXPECT_EQ(0, first_op.max_target_count());
  EXPECT_EQ(0, second_op.max_target_count());
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WDIAG");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  return RUN_ALL_TESTS();
}

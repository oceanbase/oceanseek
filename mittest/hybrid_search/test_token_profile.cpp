/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS
#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"
#include "sql/engine/ob_exec_context.h"
#undef protected
#undef private
#include "simple_server/env/ob_simple_cluster_test_base.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{

struct SearchCtxSetup
{
  explicit SearchCtxSetup(ObIAllocator &allocator)
      : scan_allocator_("TokenProfScan"),
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
    search_ctx_.eval_ctx_ = &eval_ctx_;
    search_ctx_.rowid_exprs_ = &rowid_exprs_;
    search_ctx_.output_ = &output_;
    search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs_;
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

// Two ordered postings; advance_to may return the already cached posting.
class MockProfileTokenOp : public ObIDASSearchOp
{
public:
  MockProfileTokenOp(ObDASSearchCtx &ctx, const ObDASSearchOpType type)
      : ObIDASSearchOp(ctx), cursor_(0)
  { op_type_ = type; }

private:
  int do_open() override { return OB_SUCCESS; }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override { cursor_ = 0; return OB_SUCCESS; }
  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    cursor_ += 10;
    return current(rowid, score);
  }
  int do_advance_to(const ObDASRowID &target, ObDASRowID &rowid, double &score) override
  {
    if (target.is_normal() && 999 == target.get_uint64()) {
      return OB_ERR_UNEXPECTED;
    }
    while (cursor_ <= 20 && (cursor_ == 0 || target.is_max()
        || (!target.is_min() && cursor_ < target.get_uint64()))) {
      cursor_ += 10;
    }
    return current(rowid, score);
  }
  bool can_advance_to_id() const override { return true; }
  int do_advance_to_id(const ObDASRowID &target, ObDASRowID &rowid) override
  {
    double score = 0.0;
    return do_advance_to(target, rowid, score);
  }
  int do_load_curr_score(double &score) override
  {
    ObDASRowID rowid;
    return current(rowid, score);
  }
  int current(ObDASRowID &rowid, double &score)
  {
    if (cursor_ > 20) {
      return OB_ITER_END;
    }
    rowid.set_uint64(cursor_);
    score = cursor_ / 10.0;
    return OB_SUCCESS;
  }
  uint64_t cursor_;
};

void check_token_profile(const ObDASSearchOpType type, const bool enabled)
{
  ObArenaAllocator allocator("TokenProfile");
  SearchCtxSetup ctx(allocator);
  MockProfileTokenOp op(ctx.search_ctx_, type);
  ObOpProfile<ObMetric> profile(DAS_SEARCH_OP_TOKEN == type
      ? ObProfileId::HYBRID_SEARCH_TOKEN : ObProfileId::HYBRID_SEARCH_ASYNC_TOKEN, &allocator);
  ObProfileSwitcher no_parent_profile(nullptr);
  op.profile_ = enabled ? &profile : nullptr;
  auto expect_outputs = [&](const uint64_t expected) {
    if (!enabled) {
      EXPECT_FALSE(op.has_last_profile_output_rowid_);
      return;
    }
    bool exists = false;
    uint64_t value = 0;
    profile.get_metric_value(ObMetricId::HS_OUTPUT_ROW_COUNT, exists, value);
    EXPECT_TRUE(exists);
    EXPECT_EQ(expected, value);
  };

  ObDASRowID rowid, target, boundary, after;
  double score = 0.0;
  double after_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(rowid, score));
  EXPECT_EQ(10, rowid.get_uint64());
  target.set_uint64(10);
  ASSERT_EQ(OB_SUCCESS, op.advance_to(target, rowid, score));
  EXPECT_EQ(10, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(1.0, score);
  expect_outputs(1);

  ASSERT_EQ(OB_SUCCESS, op.advance_to_id(target, rowid));
  ASSERT_EQ(OB_SUCCESS, op.load_curr_score(score));
  EXPECT_EQ(10, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(1.0, score);
  expect_outputs(1);

  // A boundary lookahead is counted once, including later cached positioning.
  uint64_t ids[2] = {0, 0};
  int64_t count = 0;
  boundary.set_uint64(15);
  ASSERT_EQ(OB_SUCCESS, op.drain_until(boundary, 2, ids, nullptr, count, after, after_score));
  EXPECT_EQ(0, count);
  EXPECT_EQ(20, after.get_uint64());
  expect_outputs(2);
  target.set_uint64(20);
  ASSERT_EQ(OB_SUCCESS, op.advance_to(target, rowid, score));
  EXPECT_EQ(20, rowid.get_uint64());
  expect_outputs(2);
  target.set_uint64(999);
  EXPECT_EQ(OB_ERR_UNEXPECTED, op.advance_to(target, rowid, score));
  EXPECT_EQ(OB_ERR_UNEXPECTED, op.advance_to_id(target, rowid));
  EXPECT_EQ(OB_ITER_END, op.next_rowid(rowid, score));
  expect_outputs(2);

  // Rescan must count the same IDs again; batch output shares the same state.
  ASSERT_EQ(OB_SUCCESS, op.rescan());
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(rowid, score));
  EXPECT_EQ(10, rowid.get_uint64());
  expect_outputs(3);
  boundary.set_uint64(20);
  ASSERT_EQ(OB_SUCCESS, op.drain_until(boundary, 2, ids, nullptr, count, after, after_score));
  ASSERT_EQ(1, count);
  EXPECT_EQ(20, ids[0]);
  EXPECT_TRUE(after.is_max());
  expect_outputs(4);

  // Active drain reuses the current row but still accumulates its score.
  ASSERT_EQ(OB_SUCCESS, op.rescan());
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(rowid, score));
  DocScoreAccumulator accumulator;
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(11, allocator));
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(10, max_id));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(10, 100.0));
  boundary.set_uint64(10);
  ASSERT_EQ(OB_SUCCESS, op.drain_until_active(boundary, accumulator));
  expect_outputs(5);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(10, score));
  EXPECT_DOUBLE_EQ(101.0, score);
  target.set_uint64(20);
  ASSERT_EQ(OB_SUCCESS, op.advance_to(target, rowid, score));
  ASSERT_EQ(OB_SUCCESS, op.advance_to(target, rowid, score));
  EXPECT_EQ(20, rowid.get_uint64());
  expect_outputs(6);
}

TEST(TestTokenProfile, token_cached_output)
{
  check_token_profile(DAS_SEARCH_OP_TOKEN, true);
}

TEST(TestTokenProfile, async_token_cached_output)
{
  check_token_profile(DAS_SEARCH_OP_ASYNC_TOKEN, true);
}

TEST(TestTokenProfile, disabled_profile)
{
  check_token_profile(DAS_SEARCH_OP_TOKEN, false);
  check_token_profile(DAS_SEARCH_OP_ASYNC_TOKEN, false);
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WDIAG");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  return RUN_ALL_TESTS();
}

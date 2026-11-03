/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#include <algorithm>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#ifdef CONST
#undef CONST
#endif
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_bmm_op.h"
#include "sql/das/search/ob_das_bmw_op.h"
#include "sql/das/search/ob_das_disjunction_op.h"
#include "sql/das/search/ob_das_req_opt_op.h"
#include "sql/das/search/ob_das_topk_collect_op.h"
#include "sql/engine/ob_exec_context.h"
#undef protected
#undef private
#include "test_das_bmm_common.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::sql::test;

namespace
{

struct FixedSeedBMMCase
{
  std::vector<ScriptedBMMPostingList> scoring_postings_;
  ScriptedBMMPostingList filter_postings_;
  std::vector<double> range_max_scores_;
  double threshold_;
};

ScriptedBMMPostingList build_scoring_postings(
    std::mt19937_64 &random,
    const int64_t dim_idx,
    const int64_t random_doc_count,
    const std::vector<uint64_t> &forced_ids,
    const double score_cap)
{
  std::map<uint64_t, double> scores;
  for (uint64_t doc_id : forced_ids) {
    if (0 == dim_idx || 0 != ((doc_id + dim_idx) % 3)) {
      scores[doc_id] = 0.5 * static_cast<double>(1 + random() % static_cast<uint64_t>(2 * score_cap));
    }
  }
  for (int64_t i = 0; i < random_doc_count; ++i) {
    const uint64_t doc_id = 1 + random() % 18000;
    scores[doc_id] = 0.5 * static_cast<double>(1 + random() % static_cast<uint64_t>(2 * score_cap));
  }
  ScriptedBMMPostingList postings;
  for (const std::pair<const uint64_t, double> &entry : scores) {
    postings.push_back(ScriptedBMMPosting(
        entry.first, entry.second, entry.first <= 16384 ? 16384 : 32768, score_cap));
  }
  return postings;
}

FixedSeedBMMCase generate_dense_diff_case(const uint64_t seed, const bool with_filter)
{
  std::mt19937_64 random(seed);
  FixedSeedBMMCase test_case;
  const std::vector<uint64_t> forced_ids = {
      1, 63, 64, 65, 255, 256, 257, 4095, 4096, 4097, 16383, 16384, 16385};
  const int64_t dim_count = 2 + static_cast<int64_t>(random() % 5);
  for (int64_t dim_idx = 0; dim_idx < dim_count; ++dim_idx) {
    const double score_cap = 2.0 + static_cast<double>(dim_idx);
    test_case.scoring_postings_.push_back(
        build_scoring_postings(random, dim_idx, 48, forced_ids, score_cap));
  }
  test_case.threshold_ = 2.0 + static_cast<double>(seed % 5);
  if (with_filter) {
    std::set<uint64_t> filter_ids;
    if (0 == seed % 3) {
      filter_ids.insert(63);
      filter_ids.insert(64);
      filter_ids.insert(65);
    } else if (1 == seed % 3) {
      // The first filter hit starts after one complete dense accumulator window.
      filter_ids.insert(4097);
      filter_ids.insert(16385);
    } else {
      filter_ids.insert(255);
      filter_ids.insert(256);
      filter_ids.insert(257);
    }
    filter_ids.insert(20000); // Keep a filter-only row in every generated script.
    for (int64_t i = 0; i < 36; ++i) {
      filter_ids.insert(1 + random() % 18000);
    }
    for (uint64_t doc_id : filter_ids) {
      test_case.filter_postings_.push_back(ScriptedBMMPosting(
          doc_id, 0.0, doc_id <= 16384 ? 16384 : 32768, 0.0));
    }
  }
  return test_case;
}

FixedSeedBMMCase generate_sparse_range_diff_case(const uint64_t seed)
{
  std::mt19937_64 random(seed);
  FixedSeedBMMCase test_case;
  const std::vector<uint64_t> forced_ids = {5, 64, 257, 4097, 16385};
  const double global_max_scores[] = {4.0, 8.0, 12.0};
  const double local_max_scores[] = {4.0, 1.0, 2.0};
  for (int64_t dim_idx = 0; dim_idx < 3; ++dim_idx) {
    test_case.scoring_postings_.push_back(build_scoring_postings(
        random, dim_idx, 32, forced_ids, local_max_scores[dim_idx]));
    ScriptedBMMPostingList &postings = test_case.scoring_postings_.back();
    ScriptedBMMPosting decisive_posting(
        5, local_max_scores[dim_idx], 16384, global_max_scores[dim_idx]);
    ScriptedBMMPostingList::iterator decisive_pos = std::lower_bound(
        postings.begin(), postings.end(), decisive_posting,
        [](const ScriptedBMMPosting &left, const ScriptedBMMPosting &right) {
          return left.doc_id_ < right.doc_id_;
        });
    if (decisive_pos == postings.end() || decisive_pos->doc_id_ != decisive_posting.doc_id_) {
      postings.insert(decisive_pos, decisive_posting);
    } else {
      decisive_pos->score_ = decisive_posting.score_;
    }
    for (ScriptedBMMPosting &posting : postings) {
      posting.block_max_score_ = global_max_scores[dim_idx];
    }
    test_case.range_max_scores_.push_back(local_max_scores[dim_idx]);
  }
  test_case.threshold_ = 6.0;
  for (uint64_t doc_id : forced_ids) {
    test_case.filter_postings_.push_back(
        ScriptedBMMPosting(doc_id, 0.0, doc_id <= 16384 ? 16384 : 32768, 0.0));
  }
  return test_case;
}

std::string format_diff_repro(
    const char *mode,
    const uint64_t seed,
    const FixedSeedBMMCase &test_case)
{
  std::ostringstream out;
  out << "BMM_DIFF_REPRO mode=" << mode << " seed=" << seed
      << " threshold=" << test_case.threshold_ << " scoring=[";
  for (size_t dim_idx = 0; dim_idx < test_case.scoring_postings_.size(); ++dim_idx) {
    if (dim_idx > 0) {
      out << ',';
    }
    out << '[';
    const ScriptedBMMPostingList &postings = test_case.scoring_postings_[dim_idx];
    for (size_t i = 0; i < postings.size(); ++i) {
      if (i > 0) {
        out << ',';
      }
      out << '(' << postings[i].doc_id_ << ',' << postings[i].score_ << ','
          << postings[i].block_end_id_ << ',' << postings[i].block_max_score_ << ')';
    }
    out << ']';
  }
  out << "] filter=[";
  for (size_t i = 0; i < test_case.filter_postings_.size(); ++i) {
    if (i > 0) {
      out << ',';
    }
    out << test_case.filter_postings_[i].doc_id_;
  }
  out << "] range_max=[";
  for (size_t i = 0; i < test_case.range_max_scores_.size(); ++i) {
    if (i > 0) {
      out << ',';
    }
    out << test_case.range_max_scores_[i];
  }
  out << ']';
  return out.str();
}

class TestDASBMMVectorized : public ::testing::Test
{
public:
  TestDASBMMVectorized()
    : allocator_("BMMVecTest"),
      scan_allocator_("BMMVecScan"),
      scan_op_(scan_allocator_),
      exec_ctx_(allocator_),
      eval_ctx_(exec_ctx_),
      rowid_exprs_(allocator_),
      output_(allocator_),
      search_ctx_(allocator_, scan_op_)
  {}

  static void SetUpTestCase()
  {
    const int ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
  }

  void SetUp() override
  {
    ObTenantEnv::get_tenant_local()->id_ = 500;
    eval_ctx_.max_batch_size_ = 256;
    scan_rtdef_.eval_ctx_ = &eval_ctx_;
    scan_op_.set_scan_rtdef(&scan_rtdef_);
    search_ctx_.eval_ctx_ = &eval_ctx_;
    search_ctx_.rowid_exprs_ = &rowid_exprs_;
    search_ctx_.output_ = &output_;
    search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs_;
    ASSERT_EQ(OB_SUCCESS, rowid_exprs_.init(1));
    ASSERT_EQ(OB_SUCCESS, rowid_exprs_.push_back(&rowid_expr_));
  }

  int init_score_op(ScriptedBMMSearchOp &op)
  {
    ScriptedBMMSearchOpParam param;
    param.set_is_scoring(true);
    return op.init(param);
  }

  int init_filter_op(ScriptedBMMSearchOp &op)
  {
    ScriptedBMMSearchOpParam param;
    return op.init(param);
  }

  int init_dense_bmm(
      ObDASBMMOp &bmm,
      const ObIArray<ObIDASSearchOp *> &score_ops,
      ObIDASSearchOp *filter_op = nullptr,
      const bool query_optional = false)
  {
    int ret = OB_SUCCESS;
    ObSEArray<ObDASSearchCost, 4> score_op_costs;
    for (int64_t i = 0; OB_SUCC(ret) && i < score_ops.count(); ++i) {
      ret = score_op_costs.push_back(ObDASSearchCost(100));
    }
    if (OB_SUCC(ret)) {
      // A non-selective lead keeps filter tests on the DENSE_WINDOW path.
      search_ctx_.table_row_count_ = ObDASSearchCost(100000);
      ObDASBMMBuildOptions param_options;
      param_options.query_optional_ = query_optional;
      param_options.lead_cost_ = ObDASSearchCost(1001);
      param_options.filter_cost_ = ObDASSearchCost(1000);
      ObDASBMMOpParam param(score_ops, filter_op, allocator_, param_options, &score_op_costs);
      param.set_is_scoring(true);
      ret = bmm.init(param);
    }
    return ret;
  }

  int init_sparse_bmm(
      ObDASBMMOp &bmm,
      const ObIArray<ObIDASSearchOp *> &score_ops,
      ObIDASSearchOp &filter_op)
  {
    int ret = OB_SUCCESS;
    ObSEArray<ObDASSearchCost, 4> score_op_costs;
    for (int64_t i = 0; OB_SUCC(ret) && i < score_ops.count(); ++i) {
      ret = score_op_costs.push_back(ObDASSearchCost(100));
    }
    if (OB_SUCC(ret)) {
      // A selective lead enables the SPARSE_SEEK path.
      search_ctx_.table_row_count_ = ObDASSearchCost(100000);
      ObDASBMMBuildOptions param_options;
      param_options.lead_cost_ = ObDASSearchCost(10);
      param_options.filter_cost_ = ObDASSearchCost(1);
      ObDASBMMOpParam param(score_ops, &filter_op, allocator_, param_options, &score_op_costs);
      param.set_is_scoring(true);
      ret = bmm.init(param);
    }
    return ret;
  }

  static int pull_all(ObDASBMMOp &bmm, std::vector<BMMReferenceResult> &results)
  {
    int ret = OB_SUCCESS;
    ObDASRowID rowid;
    double score = 0.0;
    while (OB_SUCCESS == (ret = bmm.next_rowid(rowid, score))) {
      results.push_back(BMMReferenceResult(rowid.get_uint64(), score));
    }
    return ret;
  }

  static void expect_results(
      const std::vector<BMMReferenceResult> &actual,
      const std::vector<BMMReferenceResult> &expected)
  {
    ASSERT_EQ(expected.size(), actual.size());
    std::set<uint64_t> unique_ids;
    for (int64_t i = 0; i < static_cast<int64_t>(expected.size()); ++i) {
      EXPECT_EQ(expected[i].doc_id_, actual[i].doc_id_) << "result index " << i;
      EXPECT_DOUBLE_EQ(expected[i].score_, actual[i].score_) << "result index " << i;
      if (i > 0) {
        EXPECT_LT(actual[i - 1].doc_id_, actual[i].doc_id_) << "result index " << i;
      }
      EXPECT_TRUE(unique_ids.insert(actual[i].doc_id_).second) << "duplicate rowid";
    }
  }

  int run_generated_case(
      const FixedSeedBMMCase &test_case,
      const bool vectorized,
      const bool sparse,
      std::vector<BMMReferenceResult> &results)
  {
    int ret = OB_SUCCESS;
    BMMVectorizedMergeConfigGuard config_guard(vectorized);
    std::vector<std::unique_ptr<ScriptedBMMSearchOp> > owned_score_ops;
    ObSEArray<ObIDASSearchOp *, 8> score_ops;
    for (int64_t dim_idx = 0;
        OB_SUCC(ret) && dim_idx < static_cast<int64_t>(test_case.scoring_postings_.size());
        ++dim_idx) {
      double dim_max_score = 0.0;
      for (const ScriptedBMMPosting &posting : test_case.scoring_postings_[dim_idx]) {
        dim_max_score = std::max(dim_max_score, posting.block_max_score_);
      }
      std::unique_ptr<ScriptedBMMSearchOp> op(new ScriptedBMMSearchOp(
          search_ctx_, test_case.scoring_postings_[dim_idx], dim_max_score));
      op->use_batch_drain(true);
      if (dim_idx < static_cast<int64_t>(test_case.range_max_scores_.size())) {
        op->set_range_max_score(test_case.range_max_scores_[dim_idx]);
      }
      if (OB_FAIL(init_score_op(*op))) {
      } else if (OB_FAIL(score_ops.push_back(op.get()))) {
      } else {
        owned_score_ops.push_back(std::move(op));
      }
    }

    std::unique_ptr<ScriptedBMMSearchOp> filter_op;
    if (OB_SUCC(ret) && !test_case.filter_postings_.empty()) {
      filter_op.reset(new ScriptedBMMSearchOp(search_ctx_, test_case.filter_postings_, 0.0));
      filter_op->use_batch_drain(true);
      if (OB_FAIL(init_filter_op(*filter_op))) {
      }
    }

    ObDASBMMOp bmm(search_ctx_);
    if (OB_FAIL(ret)) {
    } else if (sparse && nullptr != filter_op.get()) {
      ret = init_sparse_bmm(bmm, score_ops, *filter_op);
    } else {
      ret = init_dense_bmm(bmm, score_ops, filter_op.get());
    }
    if (OB_SUCC(ret) && bmm.enable_vec_merge_ != vectorized) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_SUCC(ret) && OB_FAIL(bmm.open())) {
    } else if (OB_SUCC(ret) && OB_FAIL(bmm.set_min_competitive_score(test_case.threshold_))) {
    } else if (OB_SUCC(ret)) {
      const int pull_ret = pull_all(bmm, results);
      ret = OB_ITER_END == pull_ret ? OB_SUCCESS : pull_ret;
    }
    if (bmm.is_inited_) {
      const int close_ret = bmm.close();
      if (OB_SUCC(ret)) {
        ret = close_ret;
      }
    }
    return ret;
  }

protected:
  ObArenaAllocator allocator_;
  ObArenaAllocator scan_allocator_;
  ObDASScanOp scan_op_;
  ObDASScanRtDef scan_rtdef_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObExpr rowid_expr_;
  ExprFixedArray rowid_exprs_;
  ExprFixedArray output_;
  ObDASSearchCtx search_ctx_;
};

// Verify that filterless vectorized dense-window merge sums overlapping scores and matches the
// thresholded oracle across batches without losing or duplicating documents, using batch drain for
// the essential dimension and active drain for the non-essential dimension.
TEST_F(TestDASBMMVectorized, dense_without_filter_matches_oracle_across_batches)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList low_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 2.0)
      .add(4097, 2.0, 20000, 2.0)
      .add(5000, 2.0, 20000, 2.0)
      .build();
  const ScriptedBMMPostingList high_postings = ScriptedBMMPostingBuilder()
      .add(1, 4.0, 20000, 8.0)
      .add(4096, 4.0, 20000, 8.0)
      .add(4097, 4.0, 20000, 8.0)
      .add(8193, 4.0, 20000, 8.0)
      .build();
  ScriptedBMMSearchOp low(search_ctx_, low_postings, 2.0);
  ScriptedBMMSearchOp high(search_ctx_, high_postings, 8.0);
  low.use_batch_drain(true);
  high.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(low));
  ASSERT_EQ(OB_SUCCESS, init_score_op(high));
  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&low));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&high));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(3.0));

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {low_postings, high_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, nullptr, 3.0));
  EXPECT_GT(high.get_call_counters().drain_, 0);
  EXPECT_GT(low.get_call_counters().active_drain_, 0);
  EXPECT_EQ(ObDASBMMOp::SearchStatus::FINISHED, bmm.status_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// In dense filter mode, materialize and score only score/filter intersections across bitset offsets
// 63 and 64, excluding both scoring-only and filter-only rows from the output.
TEST_F(TestDASBMMVectorized, dense_filter_materializes_only_intersection_across_words)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList score_postings = ScriptedBMMPostingBuilder()
      .add(1000, 1.0, 20000, 6.0)
      .add(1063, 2.0, 20000, 6.0)
      .add(1064, 3.0, 20000, 6.0)
      .add(1065, 4.0, 20000, 6.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1063, 0.0, 20000, 0.0)
      .add(1064, 0.0, 20000, 0.0)
      .add(1100, 0.0, 20000, 0.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, score_postings, 6.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  score_op.use_batch_drain(true);
  filter_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops, &filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {score_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &filter_postings, 0.0));
  EXPECT_EQ(1, filter_op.get_call_counters().drain_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// When the first dense accumulator batch has no filter hit, skip it without looping and cap
// the next scoring drain once at the first later filter hit, preserving that intersection and
// clearing the one-shot cap.
TEST_F(TestDASBMMVectorized, dense_filter_empty_window_preserves_first_later_hit)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList score_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 16384, 5.0)
      .add(2, 2.0, 16384, 5.0)
      .add(5000, 5.0, 16384, 5.0)
      .add(6000, 4.0, 16384, 5.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(5000, 0.0, 16384, 0.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, score_postings, 5.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  score_op.use_batch_drain(true);
  filter_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops, &filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {score_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &filter_postings, 0.0));
  EXPECT_FALSE(bmm.cap_next_batch_to_filter_id_);
  EXPECT_EQ(1, score_op.get_call_counters().drain_);
  EXPECT_LT(score_op.get_call_counters().advance_, 5);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// When the filter reaches EOF in the first dense batch, return its final intersections and
// finish without draining scoring postings from later windows or turning filter exhaustion
// into empty loops.
TEST_F(TestDASBMMVectorized, dense_filter_eof_stops_before_later_score_windows)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList score_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 5.0)
      .add(64, 2.0, 20000, 5.0)
      .add(5000, 3.0, 20000, 5.0)
      .add(10000, 4.0, 20000, 5.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 20000, 0.0)
      .add(64, 0.0, 20000, 0.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, score_postings, 5.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  score_op.use_batch_drain(true);
  filter_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops, &filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {score_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &filter_postings, 0.0));
  EXPECT_TRUE(bmm.filter_end_);
  EXPECT_EQ(1, score_op.get_call_counters().drain_);
  EXPECT_EQ(ObDASBMMOp::SearchStatus::FINISHED, bmm.status_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Apply non-essential dimensions in descending bound order, pruning candidates whose possible score
// is at or below the threshold and skipping lower-bound drains once all candidates are dead, while
// matching the strict-threshold oracle.
TEST_F(TestDASBMMVectorized, dense_non_essential_pruning_is_strict_and_stops_early)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList low_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 2.0)
      .add(2, 2.0, 20000, 2.0)
      .add(5000, 1.0, 20000, 2.0)
      .build();
  const ScriptedBMMPostingList high_postings = ScriptedBMMPostingBuilder()
      .add(1, 3.0, 20000, 4.0)
      .add(2, 2.0, 20000, 4.0)
      .add(5000, 4.0, 20000, 4.0)
      .build();
  const ScriptedBMMPostingList essential_postings = ScriptedBMMPostingBuilder()
      .add(1, 5.0, 20000, 12.0)
      .add(2, 6.0, 20000, 12.0)
      .add(5000, 7.0, 20000, 12.0)
      .build();
  ScriptedBMMSearchOp low(search_ctx_, low_postings, 2.0);
  ScriptedBMMSearchOp high(search_ctx_, high_postings, 4.0);
  ScriptedBMMSearchOp essential(search_ctx_, essential_postings, 12.0);
  low.use_batch_drain(true);
  high.use_batch_drain(true);
  essential.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(low));
  ASSERT_EQ(OB_SUCCESS, init_score_op(high));
  ASSERT_EQ(OB_SUCCESS, init_score_op(essential));
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&low));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&high));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&essential));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(10.0));

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {
      low_postings, high_postings, essential_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, nullptr, 10.0));
  EXPECT_EQ(2, bmm.vec_non_essential_dim_count_);
  EXPECT_GT(high.get_call_counters().active_drain_, low.get_call_counters().active_drain_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Exercise advance_to() within a materialized dense window and across its BMM boundary,
// verifying the in-window path does not rescan, the cross-window path recomputes, and
// advancing to the current row is idempotent without replaying consumed rows.
TEST_F(TestDASBMMVectorized, dense_advance_uses_current_window_and_recomputes_across_boundary)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 16384, 8.0)
      .add(10, 2.0, 16384, 8.0)
      .add(20, 3.0, 16384, 8.0)
      .add(17000, 4.0, 40000, 8.0)
      .add(17010, 5.0, 40000, 8.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, postings, 8.0);
  score_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(1.0, score);
  const int64_t shallow_before_fast_advance = score_op.get_call_counters().shallow_;
  ObDASRowID target;
  target.set_uint64(10);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(10, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(2.0, score);
  EXPECT_EQ(shallow_before_fast_advance, score_op.get_call_counters().shallow_);

  target.set_uint64(17000);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(17000, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(4.0, score);
  EXPECT_GT(score_op.get_call_counters().shallow_, shallow_before_fast_advance);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(rowid, rowid, score));
  EXPECT_EQ(17000, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(4.0, score);

  std::vector<BMMReferenceResult> tail;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, tail));
  const std::vector<ScriptedBMMPostingList> scoring = {postings};
  const std::vector<BMMReferenceResult> all_expected =
      BMMReferenceOracle::evaluate(scoring, nullptr, 0.0);
  ASSERT_EQ(5, all_expected.size());
  const std::vector<BMMReferenceResult> expected_tail = {all_expected[4]};
  expect_results(tail, expected_tail);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Raise the threshold after consuming one row from a dense window, ensuring cached and future rows
// at or below it are skipped, while lowering the threshold is rejected without changing its value.
TEST_F(TestDASBMMVectorized, dense_threshold_raise_filters_materialized_and_future_rows)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 8.0)
      .add(2, 3.0, 20000, 8.0)
      .add(3, 5.0, 20000, 8.0)
      .add(5000, 8.0, 20000, 8.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, postings, 8.0);
  score_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(1.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(3.0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bmm.set_min_competitive_score(2.0));
  EXPECT_DOUBLE_EQ(3.0, bmm.min_competitive_score_);

  std::vector<BMMReferenceResult> actual_tail;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual_tail));
  const std::vector<ScriptedBMMPostingList> scoring = {postings};
  expect_results(actual_tail, BMMReferenceOracle::evaluate(scoring, nullptr, 3.0));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// With the initial threshold, merge all three dimensions without requesting range maxima and verify
// the single result contains their full summed score.
TEST_F(TestDASBMMVectorized, initial_threshold_skips_range_max_collection)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 1.0)
      .build();
  ScriptedBMMSearchOp dim_a(search_ctx_, postings, 1.0);
  ScriptedBMMSearchOp dim_b(search_ctx_, postings, 1.0);
  ScriptedBMMSearchOp dim_c(search_ctx_, postings, 1.0);
  ScriptedBMMSearchOp *dims[] = {&dim_a, &dim_b, &dim_c};
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  for (ScriptedBMMSearchOp *dim : dims) {
    dim->set_range_max_score(1.0);
    dim->use_batch_drain(true);
    ASSERT_EQ(OB_SUCCESS, init_score_op(*dim));
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(dim));
  }
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  ASSERT_EQ(1, actual.size());
  EXPECT_EQ(1, actual[0].doc_id_);
  EXPECT_DOUBLE_EQ(3.0, actual[0].score_);
  for (ScriptedBMMSearchOp *dim : dims) {
    EXPECT_EQ(0, dim->get_call_counters().range_max_);
  }
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Set the threshold equal to the sum of all range maxima and verify strict competitiveness skips
// the entire window without draining any scoring dimension.
TEST_F(TestDASBMMVectorized, range_max_sum_equal_threshold_is_strictly_skipped)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 2.0)
      .build();
  ScriptedBMMSearchOp dim_a(search_ctx_, postings, 2.0);
  ScriptedBMMSearchOp dim_b(search_ctx_, postings, 2.0);
  ScriptedBMMSearchOp dim_c(search_ctx_, postings, 2.0);
  ScriptedBMMSearchOp *dims[] = {&dim_a, &dim_b, &dim_c};
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  for (ScriptedBMMSearchOp *dim : dims) {
    dim->set_range_max_score(1.0);
    ASSERT_EQ(OB_SUCCESS, init_score_op(*dim));
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(dim));
  }
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(3.0));

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  EXPECT_TRUE(actual.empty());
  for (ScriptedBMMSearchOp *dim : dims) {
    EXPECT_EQ(1, dim->get_call_counters().range_max_);
    EXPECT_EQ(0, dim->get_call_counters().drain_);
  }
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Raise the threshold between dense windows and verify the next window recomputes range maxima,
// repartitions three dimensions into essential/non-essential groups, and returns the surviving sum.
TEST_F(TestDASBMMVectorized, threshold_raise_repartitions_three_dimensions_across_window)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList dim_a_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 16384, 4.0)
      .add(17000, 1.0, 40000, 4.0)
      .build();
  const ScriptedBMMPostingList dim_b_postings = ScriptedBMMPostingBuilder()
      .add(1, 2.0, 16384, 4.0)
      .add(17000, 2.0, 40000, 4.0)
      .build();
  const ScriptedBMMPostingList dim_c_postings = ScriptedBMMPostingBuilder()
      .add(1, 8.0, 16384, 12.0)
      .add(17000, 12.0, 40000, 12.0)
      .build();
  ScriptedBMMSearchOp dim_a(search_ctx_, dim_a_postings, 4.0);
  ScriptedBMMSearchOp dim_b(search_ctx_, dim_b_postings, 4.0);
  ScriptedBMMSearchOp dim_c(search_ctx_, dim_c_postings, 12.0);
  dim_a.set_range_max_score(1.0);
  dim_b.set_range_max_score(2.0);
  dim_c.set_range_max_score(12.0);
  ScriptedBMMSearchOp *dims[] = {&dim_a, &dim_b, &dim_c};
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  for (ScriptedBMMSearchOp *dim : dims) {
    dim->use_batch_drain(true);
    ASSERT_EQ(OB_SUCCESS, init_score_op(*dim));
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(dim));
  }
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(11.0, score);
  for (ScriptedBMMSearchOp *dim : dims) {
    EXPECT_EQ(0, dim->get_call_counters().range_max_);
  }

  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(10.0));
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(17000, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(15.0, score);
  EXPECT_EQ(2, bmm.non_essential_dim_count_);
  EXPECT_EQ(2, bmm.vec_non_essential_dim_count_);
  for (ScriptedBMMSearchOp *dim : dims) {
    EXPECT_GT(dim->get_call_counters().range_max_, 0);
  }
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Let one dimension end in the first window and verify the next window refreshes global grouping,
// removes the exhausted bound from the non-essential suffix, and still matches the oracle.
TEST_F(TestDASBMMVectorized, range_partition_refreshes_after_dimension_eof)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList short_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 16384, 1.0)
      .build();
  const ScriptedBMMPostingList mid_postings = ScriptedBMMPostingBuilder()
      .add(1, 2.0, 16384, 2.0)
      .add(17000, 2.0, 40000, 2.0)
      .build();
  const ScriptedBMMPostingList high_postings = ScriptedBMMPostingBuilder()
      .add(1, 8.0, 16384, 8.0)
      .add(17000, 8.0, 40000, 8.0)
      .build();
  ScriptedBMMSearchOp short_dim(search_ctx_, short_postings, 1.0);
  ScriptedBMMSearchOp mid_dim(search_ctx_, mid_postings, 2.0);
  ScriptedBMMSearchOp high_dim(search_ctx_, high_postings, 8.0);
  short_dim.set_range_max_score(1.0);
  mid_dim.set_range_max_score(2.0);
  high_dim.set_range_max_score(8.0);
  ScriptedBMMSearchOp *dims[] = {&short_dim, &mid_dim, &high_dim};
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  for (ScriptedBMMSearchOp *dim : dims) {
    dim->use_batch_drain(true);
    ASSERT_EQ(OB_SUCCESS, init_score_op(*dim));
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(dim));
  }
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5.0));

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {
      short_postings, mid_postings, high_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, nullptr, 5.0));
  EXPECT_EQ(1, bmm.vec_partition_iter_end_count_);
  EXPECT_DOUBLE_EQ(2.0, bmm.non_essential_dim_max_score_);
  EXPECT_EQ(1, short_dim.get_call_counters().range_max_);
  EXPECT_GT(mid_dim.get_call_counters().range_max_, 1);
  EXPECT_GT(high_dim.get_call_counters().range_max_, 1);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Skip a range whose combined maxima cannot beat the threshold, then resume from its
// exclusive successor and return the competitive document in the next window instead of
// treating the skip as EOF.
TEST_F(TestDASBMMVectorized, range_max_skip_continues_into_next_competitive_window)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList dim_a_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.5, 100, 6.0)
      .add(20000, 4.0, 40000, 6.0)
      .build();
  const ScriptedBMMPostingList dim_b_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.5, 100, 6.0)
      .add(20000, 4.0, 40000, 6.0)
      .build();
  const ScriptedBMMPostingList dim_c_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.5, 100, 6.0)
      .add(20000, 4.0, 40000, 6.0)
      .build();
  ScriptedBMMSearchOp dim_a(search_ctx_, dim_a_postings, 6.0);
  ScriptedBMMSearchOp dim_b(search_ctx_, dim_b_postings, 6.0);
  ScriptedBMMSearchOp dim_c(search_ctx_, dim_c_postings, 6.0);
  ScriptedBMMSearchOp *dims[] = {&dim_a, &dim_b, &dim_c};
  for (ScriptedBMMSearchOp *dim : dims) {
    dim->use_batch_drain(true);
    dim->add_range_max_result(1, true, 16384, 0.5, true);
    dim->add_range_max_result(16384, false, 40000, 4.0, true);
    // Any unplanned range request must fail this contract test.
    dim->set_range_max_ret(OB_ERR_UNEXPECTED);
    ASSERT_EQ(OB_SUCCESS, init_score_op(*dim));
  }
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_b));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_c));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5.0));

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {
      dim_a_postings, dim_b_postings, dim_c_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, nullptr, 5.0));
  for (ScriptedBMMSearchOp *dim : dims) {
    EXPECT_EQ(2, dim->get_call_counters().range_max_);
  }
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Combine a calculated range maximum with can_calc=false and OB_ITER_END fallbacks, ensuring
// unavailable dimensions retain their global maxima and the final output still matches the oracle.
TEST_F(TestDASBMMVectorized, range_max_mixed_fallbacks_match_oracle)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList dim_a_postings = ScriptedBMMPostingBuilder()
      .add(10, 0.5, 20000, 2.0)
      .build();
  const ScriptedBMMPostingList dim_b_postings = ScriptedBMMPostingBuilder()
      .add(10, 1.0, 20000, 3.0)
      .build();
  const ScriptedBMMPostingList dim_c_postings = ScriptedBMMPostingBuilder()
      .add(10, 6.0, 20000, 8.0)
      .build();
  ScriptedBMMSearchOp dim_a(search_ctx_, dim_a_postings, 2.0);
  ScriptedBMMSearchOp dim_b(search_ctx_, dim_b_postings, 3.0);
  ScriptedBMMSearchOp dim_c(search_ctx_, dim_c_postings, 8.0);
  dim_a.set_range_max_score(0.5);
  dim_b.set_range_max_ret(OB_SUCCESS);
  dim_c.set_range_max_ret(OB_ITER_END);
  ScriptedBMMSearchOp *dims[] = {&dim_a, &dim_b, &dim_c};
  for (ScriptedBMMSearchOp *dim : dims) {
    dim->use_batch_drain(true);
    ASSERT_EQ(OB_SUCCESS, init_score_op(*dim));
  }
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_b));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_c));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5.0));

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {
      dim_a_postings, dim_b_postings, dim_c_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, nullptr, 5.0));
  EXPECT_DOUBLE_EQ(0.5, bmm.vec_range_max_scores_.at(0));
  EXPECT_DOUBLE_EQ(3.0, bmm.vec_range_max_scores_.at(1));
  EXPECT_DOUBLE_EQ(8.0, bmm.vec_range_max_scores_.at(2));
  for (ScriptedBMMSearchOp *dim : dims) {
    EXPECT_EQ(1, dim->get_call_counters().range_max_);
  }
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Propagate a range-max OB_IO_ERROR without publishing a partial accumulator, then clear the
// injection, rescan, and verify the complete oracle result can be produced.
TEST_F(TestDASBMMVectorized, range_max_error_propagates_and_rescan_recovers)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList dim_a_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 1.0)
      .build();
  const ScriptedBMMPostingList dim_b_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 1.0)
      .build();
  const ScriptedBMMPostingList dim_c_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 1.0)
      .build();
  ScriptedBMMSearchOp dim_a(search_ctx_, dim_a_postings, 1.0);
  ScriptedBMMSearchOp dim_b(search_ctx_, dim_b_postings, 1.0);
  ScriptedBMMSearchOp dim_c(search_ctx_, dim_c_postings, 1.0);
  dim_a.set_range_max_score(1.0);
  dim_b.set_range_max_score(1.0);
  dim_c.set_range_max_score(1.0);
  dim_b.fail_on(ScriptedBMMCallType::RANGE_MAX, 1, OB_IO_ERROR);
  ScriptedBMMSearchOp *dims[] = {&dim_a, &dim_b, &dim_c};
  for (ScriptedBMMSearchOp *dim : dims) {
    dim->use_batch_drain(true);
    ASSERT_EQ(OB_SUCCESS, init_score_op(*dim));
  }
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_b));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_c));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));

  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_IO_ERROR, bmm.next_rowid(rowid, score));
  EXPECT_TRUE(bmm.doc_score_accumulator_.is_empty());
  EXPECT_FALSE(bmm.doc_score_iter_.is_valid());

  dim_b.clear_failure();
  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));
  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {
      dim_a_postings, dim_b_postings, dim_c_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, nullptr, 2.0));
  for (ScriptedBMMSearchOp *dim : dims) {
    EXPECT_EQ(1, dim->get_call_counters().rescan_);
  }
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// In sparse mode, force range-local max-score ordering to differ from global ordering and verify
// candidate admission and completion use the local regrouping while preserving the full summed
// oracle score.
TEST_F(TestDASBMMVectorized, sparse_range_regrouping_matches_full_sum_oracle)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList dim_a_postings = ScriptedBMMPostingBuilder()
      .add(5, 10.0, 20000, 10.0)
      .build();
  const ScriptedBMMPostingList dim_b_postings = ScriptedBMMPostingBuilder()
      .add(5, 1.0, 20000, 11.0)
      .build();
  const ScriptedBMMPostingList dim_c_postings = ScriptedBMMPostingBuilder()
      .add(5, 2.0, 20000, 12.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(5, 0.0, 20000, 0.0)
      .build();
  ScriptedBMMSearchOp dim_a(search_ctx_, dim_a_postings, 10.0);
  ScriptedBMMSearchOp dim_b(search_ctx_, dim_b_postings, 11.0);
  ScriptedBMMSearchOp dim_c(search_ctx_, dim_c_postings, 12.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  dim_a.set_range_max_score(10.0);
  dim_b.set_range_max_score(1.0);
  dim_c.set_range_max_score(2.0);
  ASSERT_EQ(OB_SUCCESS, init_score_op(dim_a));
  ASSERT_EQ(OB_SUCCESS, init_score_op(dim_b));
  ASSERT_EQ(OB_SUCCESS, init_score_op(dim_c));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_b));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&dim_c));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_sparse_bmm(bmm, score_ops, filter_op));
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::SPARSE_SEEK, bmm.filter_merge_mode_);
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(12.0));

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {
      dim_a_postings, dim_b_postings, dim_c_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &filter_postings, 12.0));
  EXPECT_EQ(1, bmm.non_essential_dim_count_);
  EXPECT_EQ(2, bmm.vec_non_essential_dim_count_);
  EXPECT_EQ(1, bmm.vec_sorted_iter_idxes_.at(0));
  EXPECT_EQ(2, bmm.vec_sorted_iter_idxes_.at(1));
  EXPECT_EQ(0, bmm.vec_sorted_iter_idxes_.at(2));
  EXPECT_GT(dim_b.get_call_counters().advance_, 0);
  EXPECT_GT(dim_c.get_call_counters().advance_, 0);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Raise the threshold after partially consuming a sparse candidate buffer, ensuring stale buffered
// candidates at or below it are removed and later windows use it while competitive rows remain.
TEST_F(TestDASBMMVectorized, sparse_threshold_raise_filters_buffer_and_future_window)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList score_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 100, 8.0)
      .add(2, 3.0, 100, 8.0)
      .add(3, 5.0, 100, 8.0)
      .add(20000, 8.0, 40000, 8.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 100, 0.0)
      .add(2, 0.0, 100, 0.0)
      .add(3, 0.0, 100, 0.0)
      .add(20000, 0.0, 40000, 0.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, score_postings, 8.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_sparse_bmm(bmm, score_ops, filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(1.0, score);
  ASSERT_TRUE(bmm.sparse_candidate_buffer_.has_unconsumed());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(3.0));

  ObDASRowID target;
  target.set_uint64(2);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(3, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(5.0, score);
  std::vector<BMMReferenceResult> actual_after_raise = {
      BMMReferenceResult(rowid.get_uint64(), score)};
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual_after_raise));
  const std::vector<ScriptedBMMPostingList> scoring = {score_postings};
  expect_results(actual_after_raise,
      BMMReferenceOracle::evaluate(scoring, &filter_postings, 3.0));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Fill the sparse candidate buffer beyond its drain capacity and verify repeated refills preserve
// globally increasing output with no duplicates, omissions, or loss at the refill boundary.
TEST_F(TestDASBMMVectorized, sparse_candidate_buffer_refills_past_capacity)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  ScriptedBMMPostingList low_postings;
  ScriptedBMMPostingList essential_postings;
  ScriptedBMMPostingList filter_postings;
  for (uint64_t doc_id = 1; doc_id <= 520; ++doc_id) {
    if (0 == doc_id % 2) {
      low_postings.push_back(ScriptedBMMPosting(doc_id, 1.0, 20000, 1.0));
    }
    essential_postings.push_back(ScriptedBMMPosting(doc_id, 2.0, 20000, 2.0));
    filter_postings.push_back(ScriptedBMMPosting(doc_id, 0.0, 20000, 0.0));
  }
  ScriptedBMMSearchOp low_dim(search_ctx_, low_postings, 1.0);
  ScriptedBMMSearchOp essential_dim(search_ctx_, essential_postings, 2.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  ASSERT_EQ(OB_SUCCESS, init_score_op(low_dim));
  ASSERT_EQ(OB_SUCCESS, init_score_op(essential_dim));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&low_dim));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&essential_dim));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_sparse_bmm(bmm, score_ops, filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));

  std::vector<BMMReferenceResult> actual;
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  actual.push_back(BMMReferenceResult(rowid.get_uint64(), score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_EQ(ObDAATMergeIter::DRAIN_BATCH_SIZE,
      bmm.sparse_candidate_buffer_.get_count());
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {low_postings, essential_postings};
  const std::vector<BMMReferenceResult> expected =
      BMMReferenceOracle::evaluate(scoring, &filter_postings, 2.0);
  ASSERT_GT(expected.size(), static_cast<size_t>(ObDAATMergeIter::DRAIN_BATCH_SIZE));
  expect_results(actual, expected);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Inject OB_IO_ERROR while advancing the sparse filter, propagate it, then clear the fault,
// rescan, and reproduce the complete filtered oracle output.
TEST_F(TestDASBMMVectorized, sparse_filter_advance_error_propagates_and_rescan_recovers)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList score_postings = ScriptedBMMPostingBuilder()
      .add(5, 3.0, 100, 3.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 100, 0.0)
      .add(5, 0.0, 100, 0.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, score_postings, 3.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  filter_op.fail_on(ScriptedBMMCallType::ADVANCE, 1, OB_IO_ERROR);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_sparse_bmm(bmm, score_ops, filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_IO_ERROR, bmm.next_rowid(rowid, score));
  filter_op.clear_failure();
  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {score_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &filter_postings, 0.0));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Inject OB_IO_ERROR while aligning an essential sparse dimension to a filter candidate,
// propagate it, then clear the fault, rescan, and reproduce the thresholded oracle output.
TEST_F(TestDASBMMVectorized, sparse_essential_alignment_error_propagates_and_rescan_recovers)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList low_postings = ScriptedBMMPostingBuilder()
      .add(5, 1.0, 100, 1.0)
      .build();
  const ScriptedBMMPostingList essential_postings = ScriptedBMMPostingBuilder()
      .add(1, 2.0, 100, 2.0)
      .add(5, 2.0, 100, 2.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(5, 0.0, 100, 0.0)
      .build();
  ScriptedBMMSearchOp low_dim(search_ctx_, low_postings, 1.0);
  ScriptedBMMSearchOp essential_dim(search_ctx_, essential_postings, 2.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  essential_dim.fail_on(ScriptedBMMCallType::ADVANCE, 1, OB_IO_ERROR);
  ASSERT_EQ(OB_SUCCESS, init_score_op(low_dim));
  ASSERT_EQ(OB_SUCCESS, init_score_op(essential_dim));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&low_dim));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&essential_dim));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_sparse_bmm(bmm, score_ops, filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));

  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_IO_ERROR, bmm.next_rowid(rowid, score));
  essential_dim.clear_failure();
  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));
  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {low_postings, essential_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &filter_postings, 2.0));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Inject OB_IO_ERROR while probing a non-essential sparse dimension for score completion,
// propagate it, then clear the fault, rescan, and reproduce the thresholded oracle output.
TEST_F(TestDASBMMVectorized, sparse_non_essential_error_propagates_and_rescan_recovers)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList low_postings = ScriptedBMMPostingBuilder()
      .add(5, 1.0, 100, 1.0)
      .build();
  const ScriptedBMMPostingList essential_postings = ScriptedBMMPostingBuilder()
      .add(5, 2.0, 100, 2.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(5, 0.0, 100, 0.0)
      .build();
  ScriptedBMMSearchOp low_dim(search_ctx_, low_postings, 1.0);
  ScriptedBMMSearchOp essential_dim(search_ctx_, essential_postings, 2.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  low_dim.fail_on(ScriptedBMMCallType::ADVANCE, 1, OB_IO_ERROR);
  ASSERT_EQ(OB_SUCCESS, init_score_op(low_dim));
  ASSERT_EQ(OB_SUCCESS, init_score_op(essential_dim));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&low_dim));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&essential_dim));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_sparse_bmm(bmm, score_ops, filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));

  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_IO_ERROR, bmm.next_rowid(rowid, score));
  low_dim.clear_failure();
  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));
  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {low_postings, essential_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &filter_postings, 2.0));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// After partially consuming a thresholded dense filter window, rescan and verify the threshold,
// boundaries, filter/cap/EOF state, accumulator, and iterator are reset before reproducing the full
// zero-threshold oracle result.
TEST_F(TestDASBMMVectorized, dense_rescan_fully_resets_partial_window)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList score_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 8.0)
      .add(2, 4.0, 20000, 8.0)
      .add(3, 5.0, 20000, 8.0)
      .add(5000, 6.0, 20000, 8.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 20000, 0.0)
      .add(2, 0.0, 20000, 0.0)
      .add(3, 0.0, 20000, 0.0)
      .add(5000, 0.0, 20000, 0.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, score_postings, 8.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  score_op.use_batch_drain(true);
  filter_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops, &filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(3.0));

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(4.0, score);
  ASSERT_TRUE(bmm.doc_score_iter_.is_valid());
  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, bmm.min_competitive_score_);
  EXPECT_EQ(ObDASBMMOp::SearchStatus::COMPUTE_VEC_BOUNDARY, bmm.status_);
  EXPECT_TRUE(bmm.curr_filter_id_.is_min());
  EXPECT_TRUE(bmm.vec_boundary_id_.is_min());
  EXPECT_FALSE(bmm.doc_score_iter_.is_valid());
  EXPECT_TRUE(bmm.doc_score_accumulator_.is_empty());
  EXPECT_FALSE(bmm.cap_next_batch_to_filter_id_);
  EXPECT_FALSE(bmm.filter_end_);

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {score_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &filter_postings, 0.0));
  EXPECT_EQ(1, score_op.get_call_counters().rescan_);
  EXPECT_EQ(1, filter_op.get_call_counters().rescan_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// After partially consuming one segment, rescan into different score and filter scripts, ensuring
// prior threshold/window/filter state is cleared, each child is rescanned once, and only the second
// segment's results remain.
TEST_F(TestDASBMMVectorized, dense_segment_rescan_isolates_next_segment)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList first_score_postings = ScriptedBMMPostingBuilder()
      .add(1, 3.0, 20000, 6.0)
      .add(2, 5.0, 20000, 6.0)
      .build();
  const ScriptedBMMPostingList first_filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 20000, 0.0)
      .add(2, 0.0, 20000, 0.0)
      .build();
  const ScriptedBMMPostingList second_score_postings = ScriptedBMMPostingBuilder()
      .add(100, 1.0, 20000, 7.0)
      .add(101, 4.0, 20000, 7.0)
      .add(5000, 7.0, 20000, 7.0)
      .build();
  const ScriptedBMMPostingList second_filter_postings = ScriptedBMMPostingBuilder()
      .add(100, 0.0, 20000, 0.0)
      .add(101, 0.0, 20000, 0.0)
      .add(5000, 0.0, 20000, 0.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, first_score_postings, 7.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, first_filter_postings, 0.0);
  score_op.set_next_segment(second_score_postings);
  filter_op.set_next_segment(second_filter_postings);
  score_op.use_batch_drain(true);
  filter_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops, &filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(3.0, score);
  common::ObNewVersionRange range;
  ASSERT_EQ(OB_SUCCESS, bmm.rescan_segment(range));
  EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, bmm.min_competitive_score_);
  EXPECT_EQ(ObDASBMMOp::SearchStatus::COMPUTE_VEC_BOUNDARY, bmm.status_);
  EXPECT_TRUE(bmm.curr_filter_id_.is_min());
  EXPECT_TRUE(bmm.vec_boundary_id_.is_min());
  EXPECT_FALSE(bmm.doc_score_iter_.is_valid());
  EXPECT_TRUE(bmm.doc_score_accumulator_.is_empty());
  EXPECT_FALSE(bmm.filter_end_);

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {second_score_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &second_filter_postings, 0.0));
  EXPECT_EQ(1, score_op.get_call_counters().rescan_segment_);
  EXPECT_EQ(1, filter_op.get_call_counters().rescan_segment_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Propagate an optional dense filter drain error, then switch segments with rescan_segment(),
// clearing partial state before filter-only rows and scored intersections are returned.
TEST_F(TestDASBMMVectorized, optional_filter_error_recovers_with_segment_rescan)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList first_score_postings = ScriptedBMMPostingBuilder()
      .add(2, 2.0, 100, 2.0)
      .build();
  const ScriptedBMMPostingList first_filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 100, 0.0)
      .add(2, 0.0, 100, 0.0)
      .build();
  const ScriptedBMMPostingList second_score_postings = ScriptedBMMPostingBuilder()
      .add(10, 5.0, 100, 5.0)
      .build();
  const ScriptedBMMPostingList second_filter_postings = ScriptedBMMPostingBuilder()
      .add(9, 0.0, 100, 0.0)
      .add(10, 0.0, 100, 0.0)
      .add(11, 0.0, 100, 0.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, first_score_postings, 5.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, first_filter_postings, 0.0);
  score_op.set_next_segment(second_score_postings);
  filter_op.set_next_segment(second_filter_postings);
  score_op.use_batch_drain(true);
  filter_op.use_batch_drain(true);
  filter_op.fail_on(ScriptedBMMCallType::DRAIN, 1, OB_IO_ERROR);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops, &filter_op, true));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_TRUE(bmm.query_optional_);
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_IO_ERROR, bmm.next_rowid(rowid, score));
  filter_op.clear_failure();
  common::ObNewVersionRange range;
  ASSERT_EQ(OB_SUCCESS, bmm.rescan_segment(range));
  EXPECT_TRUE(bmm.is_filter_driven_optional());
  EXPECT_TRUE(bmm.doc_score_accumulator_.is_empty());
  EXPECT_FALSE(bmm.doc_score_iter_.is_valid());

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<BMMReferenceResult> expected = {
      BMMReferenceResult(9, 0.0),
      BMMReferenceResult(10, 5.0),
      BMMReferenceResult(11, 0.0)};
  expect_results(actual, expected);
  EXPECT_EQ(1, score_op.get_call_counters().rescan_segment_);
  EXPECT_EQ(1, filter_op.get_call_counters().rescan_segment_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Exhaust and rescan the vectorized dense BMM eight times, verifying every run completes while
// reusing steady-state allocations without increasing allocator usage.
TEST_F(TestDASBMMVectorized, repeated_rescan_reuses_vectorized_allocations)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 4.0)
      .add(4097, 2.0, 20000, 4.0)
      .add(17000, 3.0, 40000, 4.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, postings, 4.0);
  score_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const auto expected = BMMReferenceOracle::evaluate({postings}, nullptr, 0.0);
  expect_results(actual, expected);
  const int64_t steady_used = allocator_.used();
  for (int64_t round = 0; round < 8; ++round) {
    SCOPED_TRACE(round);
    ASSERT_EQ(OB_SUCCESS, bmm.rescan());
    actual.clear();
    ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
    expect_results(actual, expected);
    EXPECT_EQ(steady_used, allocator_.used());
  }
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Inject OB_IO_ERROR into the essential scoring drain and propagate it without exposing a partial
// dense result, then clear the fault, rescan, and reproduce the complete oracle output.
TEST_F(TestDASBMMVectorized, dense_essential_drain_error_propagates_and_rescan_recovers)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(1, 2.0, 20000, 5.0)
      .add(2, 3.0, 20000, 5.0)
      .add(5000, 4.0, 20000, 5.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, postings, 5.0);
  score_op.use_batch_drain(true);
  score_op.fail_on(ScriptedBMMCallType::DRAIN, 1, OB_IO_ERROR);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_IO_ERROR, bmm.next_rowid(rowid, score));
  EXPECT_FALSE(bmm.doc_score_iter_.is_valid());
  score_op.clear_failure();
  ASSERT_EQ(OB_SUCCESS, bmm.rescan());

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, nullptr, 0.0));
  EXPECT_EQ(1, score_op.get_call_counters().rescan_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Inject OB_IO_ERROR into the dense filter drain and propagate it instead of treating it as
// an empty mask, then clear the fault, rescan, and reproduce the complete filtered oracle output.
TEST_F(TestDASBMMVectorized, dense_filter_drain_error_propagates_and_rescan_recovers)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList score_postings = ScriptedBMMPostingBuilder()
      .add(1, 2.0, 20000, 5.0)
      .add(2, 3.0, 20000, 5.0)
      .add(5000, 4.0, 20000, 5.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 20000, 0.0)
      .add(5000, 0.0, 20000, 0.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, score_postings, 5.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  score_op.use_batch_drain(true);
  filter_op.use_batch_drain(true);
  filter_op.fail_on(ScriptedBMMCallType::DRAIN, 1, OB_IO_ERROR);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ASSERT_EQ(OB_SUCCESS, init_filter_op(filter_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops, &filter_op));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_IO_ERROR, bmm.next_rowid(rowid, score));
  EXPECT_TRUE(bmm.doc_score_accumulator_.is_empty());
  EXPECT_FALSE(bmm.doc_score_iter_.is_valid());
  filter_op.clear_failure();
  ASSERT_EQ(OB_SUCCESS, bmm.rescan());

  std::vector<BMMReferenceResult> actual;
  ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
  const std::vector<ScriptedBMMPostingList> scoring = {score_postings};
  expect_results(actual, BMMReferenceOracle::evaluate(scoring, &filter_postings, 0.0));
  EXPECT_EQ(1, filter_op.get_call_counters().rescan_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Run a real limit-2 TopK collector over a cached dense BMM window, ensuring heap feedback
// raises the BMM threshold monotonically, discarded rows do not reappear, and final output
// matches exhaustive top-K evaluation.
TEST_F(TestDASBMMVectorized, topk_collect_feedback_matches_exhaustive_oracle)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 20000, 8.0)
      .add(2, 2.0, 20000, 8.0)
      .add(3, 5.0, 20000, 8.0)
      .add(4, 2.5, 20000, 8.0)
      .add(5, 6.0, 20000, 8.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, postings, 8.0);
  score_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));

  ObDASTopKCollectOpParam topk_param;
  topk_param.limit_ = 2;
  topk_param.child_ = &bmm;
  topk_param.set_is_scoring(true);
  topk_param.dynamic_pruning_enabled_ = true;
  ObDASTopKCollectOp topk(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, topk.init(topk_param));
  ASSERT_TRUE(topk.dynamic_pruning_enabled_);
  ASSERT_EQ(OB_SUCCESS, topk.open());

  std::vector<BMMReferenceResult> actual;
  ObDASRowID rowid;
  double score = 0.0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = topk.next_rowid(rowid, score))) {
    actual.push_back(BMMReferenceResult(rowid.get_uint64(), score));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(2, actual.size());
  std::sort(actual.begin(), actual.end(),
      [](const BMMReferenceResult &left, const BMMReferenceResult &right) {
        return left.doc_id_ < right.doc_id_;
      });
  const std::vector<ScriptedBMMPostingList> scoring = {postings};
  std::vector<BMMReferenceResult> exhaustive =
      BMMReferenceOracle::evaluate(scoring, nullptr, 0.0);
  std::sort(exhaustive.begin(), exhaustive.end(),
      [](const BMMReferenceResult &left, const BMMReferenceResult &right) {
        return left.score_ > right.score_;
      });
  exhaustive.erase(exhaustive.begin() + 2, exhaustive.end());
  std::sort(exhaustive.begin(), exhaustive.end(),
      [](const BMMReferenceResult &left, const BMMReferenceResult &right) {
        return left.doc_id_ < right.doc_id_;
      });
  expect_results(actual, exhaustive);
  // A decreasing update would fail BMM and abort collection, so successful loading
  // plus the final heap minimum proves that feedback remained monotonic.
  EXPECT_DOUBLE_EQ(5.0, bmm.min_competitive_score_);
  EXPECT_EQ(ObDASBMMOp::SearchStatus::FINISHED, bmm.status_);
  EXPECT_EQ(OB_SUCCESS, topk.close());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Feed three equal-score rows into a limit-2 TopK collector, verifying that the initial threshold
// is published as soon as the heap becomes full and stable first-row retention is preserved.
TEST_F(TestDASBMMVectorized, topk_equal_scores_publish_threshold_when_heap_becomes_full)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(1, 5.0, 100, 5.0)
      .add(2, 5.0, 100, 5.0)
      .add(3, 5.0, 100, 5.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, postings, 5.0);
  score_op.use_batch_drain(true);
  ASSERT_EQ(OB_SUCCESS, init_score_op(score_op));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));

  ObDASTopKCollectOpParam topk_param;
  topk_param.limit_ = 2;
  topk_param.child_ = &bmm;
  topk_param.set_is_scoring(true);
  topk_param.dynamic_pruning_enabled_ = true;
  ObDASTopKCollectOp topk(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, topk.init(topk_param));
  ASSERT_EQ(OB_SUCCESS, topk.open());

  std::vector<BMMReferenceResult> actual;
  ObDASRowID rowid;
  double score = 0.0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = topk.next_rowid(rowid, score))) {
    actual.push_back(BMMReferenceResult(rowid.get_uint64(), score));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  std::sort(actual.begin(), actual.end(),
      [](const BMMReferenceResult &left, const BMMReferenceResult &right) {
        return left.doc_id_ < right.doc_id_;
      });
  ASSERT_EQ(2, actual.size());
  EXPECT_EQ(1, actual[0].doc_id_);
  EXPECT_EQ(2, actual[1].doc_id_);
  EXPECT_DOUBLE_EQ(5.0, actual[0].score_);
  EXPECT_DOUBLE_EQ(5.0, actual[1].score_);
  EXPECT_DOUBLE_EQ(5.0, bmm.min_competitive_score_);
  EXPECT_EQ(OB_SUCCESS, topk.close());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMMVectorized, nested_boolean_zero_scores_survive_initial_threshold)
{
  for (const bool vectorized : {false, true}) {
    for (const bool use_req_opt : {false, true}) {
      SCOPED_TRACE(::testing::Message() << vectorized << ", req_opt=" << use_req_opt);
      BMMVectorizedMergeConfigGuard config_guard(vectorized);
      ASSERT_TRUE(config_guard.is_valid());
      const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
          .add(1, 0.0, 100, 0.0).build();
      ScriptedBMMSearchOp filter(search_ctx_, postings, 0.0);
      ScriptedBMMSearchOp inner_empty(search_ctx_, nullptr, 0, 0.0);
      ScriptedBMMSearchOp outer_empty(search_ctx_, nullptr, 0, 0.0);
      ASSERT_EQ(OB_SUCCESS, init_filter_op(filter));
      ASSERT_EQ(OB_SUCCESS, init_score_op(inner_empty));
      ASSERT_EQ(OB_SUCCESS, init_score_op(outer_empty));
      ObSEArray<ObIDASSearchOp *, 2> inner_ops;
      ASSERT_EQ(OB_SUCCESS, inner_ops.push_back(&filter));
      ASSERT_EQ(OB_SUCCESS, inner_ops.push_back(&inner_empty));
      ObDASDisjunctionOp disjunction(search_ctx_);
      ObDASReqOptOp req_opt(search_ctx_);
      ObIDASSearchOp *inner = nullptr;
      if (use_req_opt) {
        ObDASReqOptOpParam param(&filter, &inner_empty, true);
        param.set_is_scoring(true);
        ASSERT_EQ(OB_SUCCESS, req_opt.init(param));
        inner = &req_opt;
      } else {
        ObDASDisjunctionOpParam param(inner_ops, 1, false, ObDASSearchCost(10));
        param.set_is_scoring(true);
        ASSERT_EQ(OB_SUCCESS, disjunction.init(param));
        inner = &disjunction;
      }
      ObSEArray<ObIDASSearchOp *, 2> score_ops;
      ASSERT_EQ(OB_SUCCESS, score_ops.push_back(inner));
      ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&outer_empty));
      ObDASBMMOp bmm(search_ctx_);
      ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, score_ops));
      ASSERT_EQ(OB_SUCCESS, bmm.open());
      for (int phase = 0; phase < 2; ++phase) {
        if (phase > 0) {
          ASSERT_EQ(OB_SUCCESS, bmm.rescan());
        }
        std::vector<BMMReferenceResult> actual;
        ASSERT_EQ(OB_ITER_END, pull_all(bmm, actual));
        expect_results(actual, {BMMReferenceResult(1, 0.0)});
        ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(0.0));
      }
      ASSERT_EQ(OB_SUCCESS, bmm.close());
    }
  }
}

TEST_F(TestDASBMMVectorized, optional_threshold_transition_and_lifecycle)
{
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    BMMVectorizedMergeConfigGuard config_guard(mode != 0);
    ASSERT_TRUE(config_guard.is_valid());
    const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
        .add(1, 0.0, 10000, 0.0).add(2, 0.0, 10000, 0.0)
        .add(3, 0.0, 10000, 0.0).add(4, 0.0, 10000, 0.0)
        .add(5, 0.0, 10000, 0.0).add(9000, 0.0, 10000, 0.0).build();
    const ScriptedBMMPostingList score_postings = ScriptedBMMPostingBuilder()
        .add(2, 0.0, 10000, 3.0).add(3, 1.0, 10000, 3.0)
        .add(4, 0.0, 10000, 3.0).add(5, 2.0, 10000, 3.0)
        .add(9000, 3.0, 10000, 3.0).build();
    ScriptedBMMSearchOp filter(search_ctx_, filter_postings, 0.0);
    ScriptedBMMSearchOp scoring(search_ctx_, score_postings, 3.0);
    filter.use_batch_drain(true);
    scoring.use_batch_drain(true);
    ASSERT_EQ(OB_SUCCESS, init_filter_op(filter));
    ASSERT_EQ(OB_SUCCESS, init_score_op(scoring));
    ObSEArray<ObIDASSearchOp *, 1> score_ops;
    ObSEArray<ObDASSearchCost, 1> score_costs;
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring));
    ASSERT_EQ(OB_SUCCESS, score_costs.push_back(ObDASSearchCost(100)));
    ObDASBMMBuildOptions options(mode != 0);
    options.query_optional_ = true;
    options.lead_cost_ = ObDASSearchCost(mode == 2 ? 10 : 1001);
    options.filter_cost_ = ObDASSearchCost(mode == 2 ? 1 : 1000);
    search_ctx_.table_row_count_ = ObDASSearchCost(100000);
    ObDASBMMOpParam param(score_ops, &filter, allocator_, options, &score_costs);
    param.set_is_scoring(true);
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, bmm.init(param));
    ASSERT_EQ(OB_SUCCESS, bmm.open());
    EXPECT_EQ(mode != 0, bmm.enable_vec_merge_);
    if (mode == 2) {
      EXPECT_EQ(ObDASBMMOp::FilterMergeMode::SPARSE_SEEK, bmm.filter_merge_mode_);
    }
    for (int phase = 0; phase < 3; ++phase) {
      if (phase == 1) {
        ASSERT_EQ(OB_SUCCESS, bmm.rescan());
      } else if (phase == 2) {
        common::ObNewVersionRange range;
        ASSERT_EQ(OB_SUCCESS, bmm.rescan_segment(range));
      }
      EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, bmm.min_competitive_score_);
      ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(-2.0));
      ObDASRowID rowid;
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
      EXPECT_EQ(1, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(0.0, score);
      ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(0.0));
      ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
      EXPECT_EQ(3, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(1.0, score);
      ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(1.0));
      ObDASRowID target;
      target.set_uint64(4);
      ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
      EXPECT_EQ(5, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(2.0, score);
      ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
      EXPECT_EQ(9000, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(3.0, score);
      EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
    }
    ASSERT_EQ(OB_SUCCESS, bmm.close());
    EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, bmm.min_competitive_score_);
  }
}

TEST_F(TestDASBMMVectorized, bmw_negative_threshold_preserves_zero_score_matches)
{
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 100, 1.0).add(2, 0.0, 100, 1.0)
      .add(3, 1.0, 100, 1.0).build();
  ScriptedBMMSearchOp scoring(search_ctx_, postings, 1.0);
  ASSERT_EQ(OB_SUCCESS, init_score_op(scoring));
  ObSEArray<ObIDASSearchOp *, 1> ops;
  ASSERT_EQ(OB_SUCCESS, ops.push_back(&scoring));
  ObDASBMWOpParam param(ops, 1, allocator_);
  param.set_is_scoring(true);
  ObDASBMWOp bmw(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, bmw.init(param));
  ASSERT_EQ(OB_SUCCESS, bmw.open());
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmw.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, bmw.set_min_competitive_score(-2.0));
  ASSERT_EQ(OB_SUCCESS, bmw.next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, bmw.set_min_competitive_score(0.0));
  ASSERT_EQ(OB_SUCCESS, bmw.next_rowid(rowid, score));
  EXPECT_EQ(3, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(1.0, score);
  EXPECT_EQ(OB_ITER_END, bmw.next_rowid(rowid, score));
  ASSERT_EQ(OB_SUCCESS, bmw.rescan());
  EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, bmw.min_competitive_score_);
  ASSERT_EQ(OB_SUCCESS, bmw.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, bmw.close());
}

TEST_F(TestDASBMMVectorized, topk_collects_zero_scores_before_publishing_zero_threshold)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  for (const bool pruning : {false, true}) {
    const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
        .add(1, 0.0, 100, 0.0).add(2, 0.0, 100, 0.0)
        .add(3, 0.0, 100, 0.0).build();
    ScriptedBMMSearchOp scoring(search_ctx_, postings, 0.0);
    scoring.use_batch_drain(true);
    ASSERT_EQ(OB_SUCCESS, init_score_op(scoring));
    ObSEArray<ObIDASSearchOp *, 1> ops;
    ASSERT_EQ(OB_SUCCESS, ops.push_back(&scoring));
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, init_dense_bmm(bmm, ops));
    ObDASTopKCollectOpParam param;
    param.limit_ = 2;
    param.child_ = &bmm;
    param.set_is_scoring(true);
    param.dynamic_pruning_enabled_ = pruning;
    ObDASTopKCollectOp topk(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, topk.init(param));
    ASSERT_EQ(OB_SUCCESS, topk.open());
    ObDASRowID rowid;
    double score = 0.0;
    std::set<uint64_t> ids;
    for (int i = 0; i < 2; ++i) {
      ASSERT_EQ(OB_SUCCESS, topk.next_rowid(rowid, score));
      ids.insert(rowid.get_uint64());
      EXPECT_DOUBLE_EQ(0.0, score);
    }
    EXPECT_EQ((std::set<uint64_t>{1, 2}), ids);
    EXPECT_EQ(OB_ITER_END, topk.next_rowid(rowid, score));
    EXPECT_DOUBLE_EQ(pruning ? 0.0 : DAS_SEARCH_INITIAL_COMPETITIVE_SCORE,
        bmm.min_competitive_score_);
    ASSERT_EQ(OB_SUCCESS, topk.close());
    ASSERT_EQ(OB_SUCCESS, bmm.close());
  }
}

// Run fixed-seed dense cases without filters through vectorized and scalar BMM, verifying both
// implementations match an independent thresholded oracle for every generated posting layout.
TEST_F(TestDASBMMVectorized, dense_no_filter_fixed_seed_differential)
{
  static const uint64_t seeds[] = {
      0x1001, 0x1003, 0x1007, 0x1011, 0x1013, 0x1019, 0x1021, 0x1025, 0x1031, 0x1033,
      0x1039, 0x1049, 0x1051, 0x1057, 0x1061, 0x1069, 0x1073, 0x1079, 0x1087, 0x1091};
  for (uint64_t seed : seeds) {
    const FixedSeedBMMCase test_case = generate_dense_diff_case(seed, false);
    SCOPED_TRACE(format_diff_repro("dense_no_filter", seed, test_case));
    std::vector<BMMReferenceResult> vectorized;
    std::vector<BMMReferenceResult> scalar;
    ASSERT_EQ(OB_SUCCESS, run_generated_case(test_case, true, false, vectorized));
    ASSERT_EQ(OB_SUCCESS, run_generated_case(test_case, false, false, scalar));
    const std::vector<BMMReferenceResult> expected = BMMReferenceOracle::evaluate(
        test_case.scoring_postings_, nullptr, test_case.threshold_);
    expect_results(vectorized, expected);
    expect_results(scalar, expected);
  }
}

// Run fixed-seed dense filter cases containing empty windows, bitset edges, and filter-only rows
// through vectorized and scalar BMM, verifying both match the independent filtered oracle.
TEST_F(TestDASBMMVectorized, dense_filter_fixed_seed_differential)
{
  static const uint64_t seeds[] = {
      0x2003, 0x2009, 0x2011, 0x2017, 0x2021, 0x2027,
      0x202D, 0x2035, 0x2039, 0x2041, 0x2047, 0x2051};
  for (uint64_t seed : seeds) {
    const FixedSeedBMMCase test_case = generate_dense_diff_case(seed, true);
    SCOPED_TRACE(format_diff_repro("dense_filter", seed, test_case));
    std::vector<BMMReferenceResult> vectorized;
    std::vector<BMMReferenceResult> scalar;
    ASSERT_EQ(OB_SUCCESS, run_generated_case(test_case, true, false, vectorized));
    ASSERT_EQ(OB_SUCCESS, run_generated_case(test_case, false, false, scalar));
    const std::vector<BMMReferenceResult> expected = BMMReferenceOracle::evaluate(
        test_case.scoring_postings_, &test_case.filter_postings_, test_case.threshold_);
    expect_results(vectorized, expected);
    expect_results(scalar, expected);
  }
}

// Run fixed-seed sparse filter cases whose range-local ordering differs from global ordering,
// verifying range-max regrouping and global-max fallback both match the independent oracle.
TEST_F(TestDASBMMVectorized, sparse_filter_range_regroup_fixed_seed_differential)
{
  static const uint64_t seeds[] = {
      0x3001, 0x3007, 0x3011, 0x3019, 0x3023, 0x302B, 0x3035, 0x303D};
  for (uint64_t seed : seeds) {
    const FixedSeedBMMCase test_case = generate_sparse_range_diff_case(seed);
    SCOPED_TRACE(format_diff_repro("sparse_filter_range", seed, test_case));
    std::vector<BMMReferenceResult> vectorized;
    ASSERT_EQ(OB_SUCCESS, run_generated_case(test_case, true, true, vectorized));
    const std::vector<BMMReferenceResult> expected = BMMReferenceOracle::evaluate(
        test_case.scoring_postings_, &test_case.filter_postings_, test_case.threshold_);
    expect_results(vectorized, expected);

    FixedSeedBMMCase fallback_case = test_case;
    fallback_case.range_max_scores_.clear();
    std::vector<BMMReferenceResult> fallback;
    ASSERT_EQ(OB_SUCCESS, run_generated_case(fallback_case, true, true, fallback));
    expect_results(fallback, expected);
  }
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

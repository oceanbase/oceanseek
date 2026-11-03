/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
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
#include "sql/das/search/ob_das_async_token_op.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/das/search/ob_das_filter_bitmap_provider.h"
#include "sql/das/search/ob_das_conjunction_op.h"
#include "sql/das/search/ob_disjunctive_daat_merger.h"
#include "sql/engine/ob_exec_context.h"
#undef protected
#undef private
#include "test_das_bmm_common.h"
#include "sql/engine/expr/ob_expr_bm25.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::sql::test;

namespace
{

template <typename ScoreAt>
struct TestBatchScorer
{
  const ScoreAt &score_at_;

  double operator()(const int64_t i) const
  {
    return score_at_(i);
  }

  void eval_batch(
      const int64_t start,
      const int64_t batch_count,
      double *scores) const
  {
    for (int64_t i = 0; i < batch_count; ++i) {
      scores[i] = score_at_(start + i);
    }
  }
};

struct MockBMMTokenOpParam : public ObIDASSearchOpParam
{
  MockBMMTokenOpParam() : ObIDASSearchOpParam(DAS_SEARCH_OP_DUMMY) {}
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    UNUSED(children);
    return OB_SUCCESS;
  }
};

struct MockBMMPosting
{
  MockBMMPosting()
    : doc_id_(0), score_(0.0), block_end_id_(0), block_max_score_(0.0)
  {}

  MockBMMPosting(
      const uint64_t doc_id,
      const double score,
      const uint64_t block_end_id,
      const double block_max_score)
    : doc_id_(doc_id),
      score_(score),
      block_end_id_(block_end_id),
      block_max_score_(block_max_score)
  {}

  uint64_t doc_id_;
  double score_;
  uint64_t block_end_id_;
  double block_max_score_;
};

class MockBMMTokenOp : public ObIDASSearchOp
{
public:
  MockBMMTokenOp(
      ObDASSearchCtx &search_ctx,
      const uint64_t posting_id,
      const uint64_t block_end_id,
      const double block_max_score,
      const double dim_max_score)
    : ObIDASSearchOp(search_ctx),
      single_posting_(posting_id, block_max_score, block_end_id, block_max_score),
      postings_(&single_posting_),
      posting_count_(1),
      dim_max_score_(dim_max_score),
      calc_max_score_count_(0),
      calc_max_score_ret_(OB_SUCCESS),
      advance_to_count_(0),
      advance_to_id_count_(0),
      load_curr_score_count_(0),
      enable_id_only_(false),
      current_idx_(-1),
      range_max_score_(0.0),
      calc_range_max_score_count_(0),
      can_calc_range_max_(false),
      range_max_ret_(OB_SUCCESS)
  {}

  MockBMMTokenOp(
      ObDASSearchCtx &search_ctx,
      const MockBMMPosting *postings,
      const int64_t posting_count,
      const double dim_max_score)
    : ObIDASSearchOp(search_ctx),
      single_posting_(),
      postings_(postings),
      posting_count_(posting_count),
      dim_max_score_(dim_max_score),
      calc_max_score_count_(0),
      calc_max_score_ret_(OB_SUCCESS),
      advance_to_count_(0),
      advance_to_id_count_(0),
      load_curr_score_count_(0),
      enable_id_only_(false),
      current_idx_(-1),
      range_max_score_(0.0),
      calc_range_max_score_count_(0),
      can_calc_range_max_(false),
      range_max_ret_(OB_SUCCESS)
  {}

  int64_t get_calc_max_score_count() const
  {
    return calc_max_score_count_;
  }

  void set_calc_max_score_ret(const int ret) { calc_max_score_ret_ = ret; }

  int64_t get_advance_to_count() const
  {
    return advance_to_count_;
  }

  int64_t get_advance_to_id_count() const
  {
    return advance_to_id_count_;
  }

  int64_t get_load_curr_score_count() const
  {
    return load_curr_score_count_;
  }

  void enable_advance_to_id()
  {
    enable_id_only_ = true;
  }

  bool can_advance_to_id() const override
  {
    return enable_id_only_;
  }

  void set_range_max_score(const double range_max_score)
  {
    range_max_score_ = range_max_score;
    can_calc_range_max_ = true;
    range_max_ret_ = OB_SUCCESS;
  }

  void set_range_max_ret(const int range_max_ret)
  {
    can_calc_range_max_ = false;
    range_max_ret_ = range_max_ret;
  }

  int64_t get_calc_range_max_score_count() const
  {
    return calc_range_max_score_count_;
  }

private:
  int do_open() override
  {
    int ret = OB_SUCCESS;
    current_idx_ = -1;
    if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    }
    return ret;
  }

  int do_close() override
  {
    return OB_SUCCESS;
  }

  int do_rescan() override
  {
    int ret = OB_SUCCESS;
    current_idx_ = -1;
    if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    }
    return ret;
  }

  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    int ret = OB_SUCCESS;
    if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (current_idx_ + 1 >= posting_count_) {
      current_idx_ = posting_count_;
      ret = OB_ITER_END;
    } else {
      ++current_idx_;
      set_current_row(rowid, score);
    }
    return ret;
  }

  int do_advance_to(const ObDASRowID &target, ObDASRowID &rowid, double &score) override
  {
    int ret = OB_SUCCESS;
    int64_t idx = current_idx_ < 0 ? 0 : current_idx_;
    ++advance_to_count_;
    if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while (idx < posting_count_ && postings_[idx].doc_id_ < target.get_uint64()) {
        ++idx;
      }
    }
    if (OB_SUCC(ret) && idx >= posting_count_) {
      current_idx_ = posting_count_;
      ret = OB_ITER_END;
    } else if (OB_SUCC(ret)) {
      current_idx_ = idx;
      set_current_row(rowid, score);
    }
    return ret;
  }

  int do_advance_to_id(const ObDASRowID &target, ObDASRowID &rowid) override
  {
    int ret = OB_SUCCESS;
    int64_t idx = current_idx_ < 0 ? 0 : current_idx_;
    ++advance_to_id_count_;
    if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while (idx < posting_count_ && postings_[idx].doc_id_ < target.get_uint64()) {
        ++idx;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (idx >= posting_count_) {
      current_idx_ = posting_count_;
      ret = OB_ITER_END;
    } else {
      current_idx_ = idx;
      rowid.set_uint64(postings_[idx].doc_id_);
    }
    return ret;
  }

  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override
  {
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    max_score_tuple = nullptr;
    if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while (idx < posting_count_
          && (postings_[idx].block_end_id_ < target.get_uint64()
              || (!inclusive && postings_[idx].block_end_id_ == target.get_uint64()))) {
        ++idx;
      }
    }
    if (OB_SUCC(ret) && idx >= posting_count_) {
      ret = OB_ITER_END;
    } else if (OB_SUCC(ret)) {
      ObDASRowID block_end_id;
      block_end_id.set_uint64(postings_[idx].block_end_id_);
      max_score_tuple_.set(target, block_end_id, postings_[idx].block_max_score_);
      max_score_tuple = &max_score_tuple_;
    }
    return ret;
  }

  int do_load_curr_score(double &score) override
  {
    int ret = OB_SUCCESS;
    ++load_curr_score_count_;
    if (OB_UNLIKELY(!is_valid_posting_array() || current_idx_ < 0
        || current_idx_ >= posting_count_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      score = postings_[current_idx_].score_;
    }
    return ret;
  }

  int do_calc_max_score(double &max_score) override
  {
    // 记录底层全局上界计算次数，供测试判断 BMM 是否重复读取 dim_max_score。
    ++calc_max_score_count_;
    // 返回 mock token 在整个 doc_id 空间上的全局分数上界。
    max_score = dim_max_score_;
    return calc_max_score_ret_;
  }

  int do_calc_range_max_score(
      const ObDASRowID &start_id,
      const bool start_inclusive,
      const ObDASRowID &end_id,
      double &max_score,
      bool &can_calc) override
  {
    UNUSED(start_id);
    UNUSED(start_inclusive);
    UNUSED(end_id);
    ++calc_range_max_score_count_;
    // Return the configured range-max result to cover unavailable and end fallbacks.
    const int ret = range_max_ret_;
    can_calc = OB_SUCCESS == ret && can_calc_range_max_;
    if (can_calc) {
      // Supply the configured score upper bound for the current window.
      max_score = range_max_score_;
    }
    return ret;
  }

  bool is_valid_posting_array() const
  {
    return posting_count_ >= 0 && (0 == posting_count_ || nullptr != postings_);
  }

  void set_current_row(ObDASRowID &rowid, double &score) const
  {
    rowid.set_uint64(postings_[current_idx_].doc_id_);
    score = postings_[current_idx_].score_;
  }

private:
  MockBMMPosting single_posting_;
  const MockBMMPosting *postings_;
  int64_t posting_count_;
  double dim_max_score_;
  int64_t calc_max_score_count_;
  int calc_max_score_ret_;
  int64_t advance_to_count_;
  int64_t advance_to_id_count_;
  int64_t load_curr_score_count_;
  bool enable_id_only_;
  int64_t current_idx_;
  double range_max_score_;
  int64_t calc_range_max_score_count_;
  bool can_calc_range_max_;
  int range_max_ret_;
};

class MockEmptyBMMTokenOp : public MockBMMTokenOp
{
public:
  explicit MockEmptyBMMTokenOp(ObDASSearchCtx &search_ctx)
    : MockBMMTokenOp(search_ctx, 0, 0, 0.0, 0.0)
  {}

private:
  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    UNUSED(score);
    rowid.set_max();
    return OB_ITER_END;
  }

  int do_advance_to(const ObDASRowID &target, ObDASRowID &rowid, double &score) override
  {
    UNUSED(target);
    UNUSED(score);
    rowid.set_max();
    return OB_ITER_END;
  }
};

class MockDirectBMMFilterOp : public ObIDASSearchOp
{
public:
  MockDirectBMMFilterOp(ObDASSearchCtx &search_ctx, const uint64_t rowid)
    : ObIDASSearchOp(search_ctx),
      single_rowid_(rowid),
      rowids_(&single_rowid_),
      rowid_count_(1),
      next_rowid_idx_(0),
      last_range_start_(0),
      last_range_end_(0),
      drain_count_(0)
  {}

  MockDirectBMMFilterOp(
      ObDASSearchCtx &search_ctx, const uint64_t *rowids, const int64_t rowid_count)
    : MockDirectBMMFilterOp(search_ctx, 0)
  {
    rowids_ = rowids;
    rowid_count_ = rowid_count;
  }

  bool supports_direct_rowid_drain() const override { return true; }
  bool can_enumerate_rowids() const override { return false; }
  uint64_t get_last_range_start() const { return last_range_start_; }
  uint64_t get_last_range_end() const { return last_range_end_; }
  int64_t get_drain_count() const { return drain_count_; }

private:
  int do_open() override
  {
    drain_count_ = 0;
    next_rowid_idx_ = 0;
    return OB_SUCCESS;
  }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override
  {
    drain_count_ = 0;
    next_rowid_idx_ = 0;
    return OB_SUCCESS;
  }
  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    UNUSEDx(rowid, score);
    return OB_NOT_SUPPORTED;
  }
  int do_advance_to(
      const ObDASRowID &target,
      ObDASRowID &rowid,
      double &score) override
  {
    UNUSEDx(target, rowid, score);
    return OB_NOT_SUPPORTED;
  }
  int do_direct_drain_rowids(
      const uint64_t range_start,
      const uint64_t range_end,
      ObDASRowIDBatchConsumer &consumer,
      ObDASRowID &next_id_after) override
  {
    int ret = OB_SUCCESS;
    next_id_after.set_max();
    last_range_start_ = range_start;
    last_range_end_ = range_end;
    ++drain_count_;
    if (OB_UNLIKELY(range_start > range_end || rowid_count_ < 0
        || (rowid_count_ > 0 && nullptr == rowids_))) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while (next_rowid_idx_ < rowid_count_ && rowids_[next_rowid_idx_] < range_start) {
        ++next_rowid_idx_;
      }
      const int64_t start_idx = next_rowid_idx_;
      int64_t end_idx = start_idx;
      while (end_idx < rowid_count_ && rowids_[end_idx] <= range_end) {
        ++end_idx;
      }
      if (end_idx > start_idx
          && OB_FAIL(consumer.consume(rowids_ + start_idx, end_idx - start_idx))) {
      } else {
        next_rowid_idx_ = end_idx;
        if (next_rowid_idx_ < rowid_count_) {
          next_id_after.set_uint64(rowids_[next_rowid_idx_]);
        }
      }
    }
    return ret;
  }

private:
  uint64_t single_rowid_;
  const uint64_t *rowids_;
  int64_t rowid_count_;
  int64_t next_rowid_idx_;
  uint64_t last_range_start_;
  uint64_t last_range_end_;
  int64_t drain_count_;
};

class MockCallbackUnorderedDirectBMMFilterOp : public ObIDASSearchOp
{
public:
  explicit MockCallbackUnorderedDirectBMMFilterOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx), emitted_(false)
  {}

  bool supports_direct_rowid_drain() const override { return true; }

private:
  int do_open() override
  {
    emitted_ = false;
    return OB_SUCCESS;
  }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override
  {
    emitted_ = false;
    return OB_SUCCESS;
  }
  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    UNUSEDx(rowid, score);
    return OB_NOT_SUPPORTED;
  }
  int do_advance_to(
      const ObDASRowID &target,
      ObDASRowID &rowid,
      double &score) override
  {
    UNUSEDx(target, rowid, score);
    return OB_NOT_SUPPORTED;
  }
  int do_direct_drain_rowids(
      const uint64_t range_start,
      const uint64_t range_end,
      ObDASRowIDBatchConsumer &consumer,
      ObDASRowID &next_id_after) override
  {
    int ret = OB_SUCCESS;
    static const uint64_t FIRST_CALLBACK[] = {105};
    static const uint64_t SECOND_CALLBACK[] = {101, 103, 105};
    next_id_after.set_max();
    if (OB_UNLIKELY(range_start > 101 || range_end < 105)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!emitted_ && OB_FAIL(consumer.consume(
        FIRST_CALLBACK, ARRAYSIZEOF(FIRST_CALLBACK)))) {
    } else if (!emitted_ && OB_FAIL(consumer.consume(
        SECOND_CALLBACK, ARRAYSIZEOF(SECOND_CALLBACK)))) {
    } else {
      emitted_ = true;
    }
    return ret;
  }

private:
  bool emitted_;
};

class MockEnumerableDirectBMMFilterOp : public MockDirectBMMFilterOp
{
public:
  using MockDirectBMMFilterOp::MockDirectBMMFilterOp;
  bool can_enumerate_rowids() const override { return true; }
};

class MockBMMPostingOp : public ObIDASSearchOp
{
public:
  MockBMMPostingOp(
      ObDASSearchCtx &search_ctx,
      const uint64_t *posting_ids,
      const double *scores,
      const int64_t posting_count)
    : ObIDASSearchOp(search_ctx),
      posting_ids_(posting_ids),
      scores_(scores),
      posting_count_(posting_count),
      curr_idx_(-1),
      max_score_(0.0),
      drain_count_(0),
      saw_score_buffer_(false)
  {
    for (int64_t i = 0; i < posting_count_; ++i) {
      max_score_ = MAX(max_score_, scores_[i]);
    }
  }

  int64_t get_drain_count() const { return drain_count_; }
  bool saw_score_buffer() const { return saw_score_buffer_; }

private:
  int do_open() override
  {
    curr_idx_ = -1;
    drain_count_ = 0;
    saw_score_buffer_ = false;
    return OB_SUCCESS;
  }

  int do_close() override { return OB_SUCCESS; }

  int do_rescan() override
  {
    curr_idx_ = -1;
    drain_count_ = 0;
    saw_score_buffer_ = false;
    return OB_SUCCESS;
  }

  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    int ret = OB_SUCCESS;
    ++curr_idx_;
    if (curr_idx_ >= posting_count_) {
      ret = OB_ITER_END;
    } else {
      rowid.set_uint64(posting_ids_[curr_idx_]);
      score = scores_[curr_idx_];
    }
    return ret;
  }

  int do_advance_to(const ObDASRowID &target, ObDASRowID &rowid, double &score) override
  {
    int ret = OB_SUCCESS;
    const uint64_t target_id = target.get_uint64();
    curr_idx_ = MAX(curr_idx_, static_cast<int64_t>(0));
    while (curr_idx_ < posting_count_
           && posting_ids_[curr_idx_] < target_id) {
      ++curr_idx_;
    }
    if (curr_idx_ >= posting_count_) {
      ret = OB_ITER_END;
    } else {
      rowid.set_uint64(posting_ids_[curr_idx_]);
      score = scores_[curr_idx_];
    }
    return ret;
  }

  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override
  {
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    const uint64_t target_id = target.get_uint64();
    while (idx < posting_count_
           && (posting_ids_[idx] < target_id
               || (!inclusive && posting_ids_[idx] == target_id))) {
      ++idx;
    }
    if (idx >= posting_count_) {
      ret = OB_ITER_END;
    } else {
      ObDASRowID block_end;
      block_end.set_uint64(posting_ids_[posting_count_ - 1]);
      max_score_tuple_.set(target, block_end, max_score_);
      max_score_tuple = &max_score_tuple_;
    }
    return ret;
  }

  int do_calc_max_score(double &max_score) override
  {
    max_score = max_score_;
    return OB_SUCCESS;
  }

  int do_drain_until(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after) override
  {
    ++drain_count_;
    saw_score_buffer_ = saw_score_buffer_ || OB_NOT_NULL(out_scores);
    return ObIDASSearchOp::do_drain_until(
        boundary, capacity, out_ids, out_scores, count,
        next_id_after, next_score_after);
  }

private:
  const uint64_t *posting_ids_;
  const double *scores_;
  int64_t posting_count_;
  int64_t curr_idx_;
  double max_score_;
  int64_t drain_count_;
  bool saw_score_buffer_;
};

class TestDASBMM : public ::testing::Test
{
public:
  TestDASBMM()
    : allocator_("BMMTest"),
      scan_allocator_("BMMScan"),
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

  int init_bmm_with_costs(
      ObDASBMMOp &bmm,
      const ObIArray<ObIDASSearchOp *> &score_ops,
      ObIDASSearchOp *filter_op,
      const int64_t table_row_count,
      const int64_t lead_cost,
      const bool query_optional = false,
      const int64_t filter_cost = 1)
  {
    int ret = OB_SUCCESS;
    ObSEArray<ObDASSearchCost, 4> score_op_costs;
    for (int64_t i = 0; OB_SUCC(ret) && i < score_ops.count(); ++i) {
      ret = score_op_costs.push_back(ObDASSearchCost(100));
    }
    if (OB_SUCC(ret)) {
      search_ctx_.table_row_count_ = ObDASSearchCost(table_row_count);
      ObDASBMMBuildOptions bmm_param_options;
      bmm_param_options.query_optional_ = query_optional;
      bmm_param_options.lead_cost_ = ObDASSearchCost(lead_cost);
      bmm_param_options.filter_cost_ = ObDASSearchCost(filter_cost);
      ObDASBMMOpParam bmm_param(
          score_ops, filter_op, allocator_, bmm_param_options, &score_op_costs);
      bmm_param.set_is_scoring(true);
      ret = bmm.init(bmm_param);
    }
    return ret;
  }

  void verify_provider_word_windows(const bool query_optional)
  {
    BMMVectorizedMergeConfigGuard config_guard;
    ASSERT_TRUE(config_guard.is_valid());
    eval_ctx_.max_batch_size_ = 1;
    const uint64_t scoring_ids[] = {2, 3, 7, 11, 18, 33};
    const double scoring_scores[] = {2.0, 3.0, 7.0, 11.0, 18.0, 33.0};
    const uint64_t filter_ids[] = {1, 2, 7, 11, 33, 40};
    const double filter_scores[] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    MockBMMTokenOpParam score_param;
    MockBMMTokenOpParam filter_param;
    score_param.set_is_scoring(true);
    MockBMMPostingOp scoring_op(
        search_ctx_, scoring_ids, scoring_scores, ARRAYSIZEOF(scoring_ids));
    MockBMMPostingOp filter_source(
        search_ctx_, filter_ids, filter_scores, ARRAYSIZEOF(filter_ids));
    ASSERT_EQ(OB_SUCCESS, scoring_op.init(score_param));
    ASSERT_EQ(OB_SUCCESS, filter_source.init(filter_param));
    ObDASFilterBitmapProvider *provider = nullptr;
    ObDASBitmapOp *view = nullptr;
    ASSERT_EQ(OB_SUCCESS, search_ctx_.create_filter_bitmap_provider(
        reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
        DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
        &filter_source, provider));
    ASSERT_NE(nullptr, provider);
    ASSERT_EQ(OB_SUCCESS, provider->create_view(search_ctx_, view));
    ASSERT_NE(nullptr, view);
    ObSEArray<ObIDASSearchOp *, 1> score_ops;
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring_op));
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
        bmm, score_ops, view, 1000, 11, query_optional));
    ASSERT_TRUE(bmm.enable_vec_merge_);
    ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
    ASSERT_EQ(16, bmm.doc_score_accumulator_.get_capacity());
    ASSERT_EQ(OB_SUCCESS, bmm.open());
    const uint64_t expected_ids[] = {1, 2, 7, 11, 33, 40};
    const double expected_scores[] = {0.0, 2.0, 7.0, 11.0, 33.0, 0.0};
    const int64_t begin = query_optional ? 0 : 1;
    const int64_t end = query_optional ? ARRAYSIZEOF(expected_ids) : 5;
    ObDASRowID rowid;
    double score = -1.0;
    for (int64_t i = begin; i < end; ++i) {
      ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
      EXPECT_EQ(expected_ids[i], rowid.get_uint64());
      EXPECT_DOUBLE_EQ(expected_scores[i], score);
    }
    EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
    EXPECT_TRUE(provider->is_prepared());
    ASSERT_NE(nullptr, provider->get_bitmap());
    EXPECT_EQ(6, provider->get_bitmap()->cardinality());
    EXPECT_EQ(OB_SUCCESS, bmm.close());
    search_ctx_.reset();
  }

  int prepare_vec_runtime_arrays(ObDASBMMOp &bmm, const int64_t dim_count)
  {
    int ret = OB_SUCCESS;
    bmm.non_ess_block_max_score_suffix_sums_.set_allocator(&allocator_);
    bmm.vec_sorted_iter_idxes_.set_allocator(&allocator_);
    bmm.vec_range_max_scores_.set_allocator(&allocator_);
    bmm.score_op_probe_allowed_.set_allocator(&allocator_);
    if (OB_FAIL(bmm.non_ess_block_max_score_suffix_sums_.prepare_allocate(dim_count))) {
      LOG_WARN("failed to prepare non essential block max score suffix sums", K(ret), K(dim_count));
    } else if (OB_FAIL(bmm.vec_sorted_iter_idxes_.prepare_allocate(dim_count))) {
      LOG_WARN("failed to prepare vectorized iterator order", K(ret), K(dim_count));
    } else if (OB_FAIL(bmm.vec_range_max_scores_.prepare_allocate(dim_count))) {
      LOG_WARN("failed to prepare vectorized range max scores", K(ret), K(dim_count));
    } else if (OB_FAIL(bmm.score_op_probe_allowed_.prepare_allocate(dim_count))) {
      LOG_WARN("failed to prepare score op probe gates", K(ret), K(dim_count));
    } else {
      for (int64_t i = 0; i < dim_count; ++i) {
        bmm.non_ess_block_max_score_suffix_sums_.at(i) = 0.0;
        bmm.vec_sorted_iter_idxes_.at(i) = i;
        bmm.vec_range_max_scores_.at(i) = 0.0;
        bmm.score_op_probe_allowed_.at(i) = false;
      }
    }
    return ret;
  }

  void SetUp() override
  {
    // 初始化真实 BMM merger 使用的 uint64 row-id 上下文。
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
  }

protected:
  ObArenaAllocator allocator_;
  ObArenaAllocator scan_allocator_;
  ObDASScanOp scan_op_;
  ObDASScanRtDef scan_rtdef_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ExprFixedArray rowid_exprs_;
  ExprFixedArray output_;
  ObDASSearchCtx search_ctx_;
};

TEST_F(TestDASBMM, rowid_only_drain_uses_null_score_buffer_across_full_and_tail_batches)
{
  static const int64_t POSTING_COUNT = ObDAATMergeIter::DRAIN_BATCH_SIZE + 3;
  uint64_t posting_ids[POSTING_COUNT];
  double scores[POSTING_COUNT];
  for (int64_t i = 0; i < POSTING_COUNT; ++i) {
    posting_ids[i] = static_cast<uint64_t>(i);
    scores[i] = static_cast<double>(i);
  }
  MockBMMTokenOpParam param;
  param.set_is_scoring(false);
  MockBMMPostingOp filter_op(
      search_ctx_, posting_ids, scores, POSTING_COUNT);
  ASSERT_EQ(OB_SUCCESS, filter_op.init(param));
  ASSERT_EQ(OB_SUCCESS, filter_op.open());

  ObDASRowID curr_id;
  double ignored_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, filter_op.next_rowid(curr_id, ignored_score));
  ASSERT_EQ(0, curr_id.get_uint64());
  ObFastBitmap bitmap(allocator_);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(POSTING_COUNT));
  ObFastBitmapRowIDConsumer sink(bitmap);
  ObDASRowID boundary;
  boundary.set_uint64(ObDAATMergeIter::DRAIN_BATCH_SIZE + 1);
  bool iter_end = false;
  ASSERT_EQ(OB_SUCCESS, drain_search_op_rowids_until(
      filter_op, curr_id, iter_end, boundary, sink));
  EXPECT_FALSE(iter_end);
  EXPECT_EQ(ObDAATMergeIter::DRAIN_BATCH_SIZE + 2, sink.consumed_count());
  EXPECT_EQ(2, filter_op.get_drain_count());
  EXPECT_FALSE(filter_op.saw_score_buffer());
  ASSERT_TRUE(curr_id.is_normal());
  EXPECT_EQ(ObDAATMergeIter::DRAIN_BATCH_SIZE + 2, curr_id.get_uint64());

  boundary.set_uint64(ObDAATMergeIter::DRAIN_BATCH_SIZE + 2);
  ASSERT_EQ(OB_SUCCESS, drain_search_op_rowids_until(
      filter_op, curr_id, iter_end, boundary, sink));
  EXPECT_TRUE(iter_end);
  EXPECT_EQ(POSTING_COUNT, sink.consumed_count());
  EXPECT_EQ(3, filter_op.get_drain_count());
  EXPECT_FALSE(filter_op.saw_score_buffer());
  bitmap.finalize();
  EXPECT_EQ(POSTING_COUNT, bitmap.cardinality());
  EXPECT_EQ(OB_SUCCESS, filter_op.close());
}

TEST_F(TestDASBMM, rowid_only_drain_rejects_in_range_lookahead)
{
  class InvalidLookaheadPostingOp : public MockBMMPostingOp
  {
  public:
    using MockBMMPostingOp::MockBMMPostingOp;

  private:
    int do_drain_until(
        const ObDASRowID &boundary,
        int64_t capacity,
        uint64_t *out_ids,
        double *out_scores,
        int64_t &count,
        ObDASRowID &next_id_after,
        double &next_score_after) override
    {
      UNUSEDx(capacity, out_ids, out_scores);
      count = 0;
      if (returned_invalid_lookahead_) {
        next_id_after.set_max();
      } else {
        next_id_after = boundary;
        returned_invalid_lookahead_ = true;
      }
      next_score_after = 0.0;
      return OB_SUCCESS;
    }

    bool returned_invalid_lookahead_ = false;
  };

  const uint64_t posting_ids[] = {0, 1};
  const double scores[] = {0.0, 0.0};
  MockBMMTokenOpParam param;
  param.set_is_scoring(false);
  InvalidLookaheadPostingOp filter_op(
      search_ctx_, posting_ids, scores, ARRAYSIZEOF(posting_ids));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(param));
  ASSERT_EQ(OB_SUCCESS, filter_op.open());

  ObDASRowID curr_id;
  ObDASRowID boundary;
  double ignored_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, filter_op.next_rowid(curr_id, ignored_score));
  ASSERT_EQ(posting_ids[0], curr_id.get_uint64());
  boundary.set_uint64(posting_ids[ARRAYSIZEOF(posting_ids) - 1]);
  bool iter_end = false;
  ObFastBitmap bitmap(allocator_);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(ARRAYSIZEOF(posting_ids)));
  ObFastBitmapRowIDConsumer sink(bitmap);
  ASSERT_EQ(OB_ERR_UNEXPECTED, drain_search_op_rowids_until(
      filter_op, curr_id, iter_end, boundary, sink));
  EXPECT_FALSE(iter_end);
  EXPECT_EQ(1, sink.consumed_count());
  bitmap.finalize();
  EXPECT_EQ(1, bitmap.cardinality());
  EXPECT_EQ(OB_SUCCESS, filter_op.close());
}

TEST_F(TestDASBMM, direct_filter_provider_consumes_unordered_duplicate_batches)
{
  MockBMMTokenOpParam param;
  param.set_is_scoring(false);
  MockCallbackUnorderedDirectBMMFilterOp source(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, source.init(param));
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      &source, provider));
  ASSERT_NE(nullptr, provider);
  ASSERT_EQ(OB_SUCCESS, provider->ensure_prepared());
  ASSERT_NE(nullptr, provider->get_bitmap());
  EXPECT_EQ(3, provider->get_bitmap()->cardinality());
  ObFastBitmap::Iterator bitmap_iter;
  ASSERT_EQ(OB_SUCCESS, bitmap_iter.init(provider->get_bitmap()));
  const uint64_t expected[] = {101, 103, 105};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected); ++i) {
    uint64_t rowid = 0;
    ASSERT_EQ(OB_SUCCESS, bitmap_iter.next_id(rowid));
    EXPECT_EQ(expected[i], rowid);
  }
  uint64_t rowid = 0;
  EXPECT_EQ(OB_ITER_END, bitmap_iter.next_id(rowid));
  EXPECT_EQ(OB_SUCCESS, source.close());
  search_ctx_.reset();
}

TEST_F(TestDASBMM, direct_filter_provider_respects_docid_range)
{
  const uint64_t rowids[] = {100, 102, 104, 106};
  MockBMMTokenOpParam param;
  param.set_is_scoring(false);
  MockDirectBMMFilterOp source(search_ctx_, rowids, ARRAYSIZEOF(rowids));
  ASSERT_EQ(OB_SUCCESS, source.init(param));
  ObObj range_lo;
  ObObj range_hi;
  range_lo.set_uint64(102);
  range_hi.set_uint64(104);
  search_ctx_.set_docid_range(range_lo, range_hi);

  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_GROUP_SCOPE, 0, &source, provider));
  ASSERT_NE(nullptr, provider);
  ASSERT_EQ(OB_SUCCESS, provider->ensure_prepared());
  EXPECT_EQ(102, source.get_last_range_start());
  EXPECT_EQ(104, source.get_last_range_end());
  ASSERT_NE(nullptr, provider->get_bitmap());
  EXPECT_EQ(2, provider->get_bitmap()->cardinality());
  ObFastBitmap::Iterator bitmap_iter;
  ASSERT_EQ(OB_SUCCESS, bitmap_iter.init(provider->get_bitmap()));
  uint64_t rowid = 0;
  ASSERT_EQ(OB_SUCCESS, bitmap_iter.next_id(rowid));
  EXPECT_EQ(102, rowid);
  ASSERT_EQ(OB_SUCCESS, bitmap_iter.next_id(rowid));
  EXPECT_EQ(104, rowid);
  EXPECT_EQ(OB_ITER_END, bitmap_iter.next_id(rowid));
  EXPECT_EQ(OB_SUCCESS, source.close());
  search_ctx_.reset();
}

TEST_F(TestDASBMM, sort_max_scores_stops_on_error_and_retries)
{
  MockBMMTokenOpParam token_param;
  MockBMMTokenOp token_a(search_ctx_, 1, 10, 1.0, 3.0);
  MockBMMTokenOp token_b(search_ctx_, 1, 10, 1.0, 1.0);
  MockBMMTokenOp token_c(search_ctx_, 1, 10, 1.0, 2.0);
  ASSERT_EQ(OB_SUCCESS, token_a.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_b.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_c.init(token_param));
  token_b.set_calc_max_score_ret(OB_TIMEOUT);

  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_b));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_c));
  ObDASBMMOp bmm(search_ctx_);
  ObDASBMMOpParam bmm_param(score_ops, nullptr, allocator_, ObDASBMMBuildOptions());
  ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
  ASSERT_EQ(3, bmm.dim_max_scores_.count());

  EXPECT_EQ(OB_TIMEOUT, bmm.sort_iters_by_max_score());
  EXPECT_FALSE(bmm.iters_sorted_);
  EXPECT_EQ(1, token_b.get_calc_max_score_count());
  EXPECT_EQ(0, token_c.get_calc_max_score_count());
  for (int64_t i = 0; i < score_ops.count(); ++i) {
    EXPECT_EQ(i, bmm.sorted_iter_idxes_.at(i));
  }

  token_b.set_calc_max_score_ret(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
  EXPECT_TRUE(bmm.iters_sorted_);
  EXPECT_DOUBLE_EQ(3.0, bmm.dim_max_scores_.at(0));
  EXPECT_DOUBLE_EQ(1.0, bmm.dim_max_scores_.at(1));
  EXPECT_DOUBLE_EQ(2.0, bmm.dim_max_scores_.at(2));
  EXPECT_EQ(1, bmm.sorted_iter_idxes_.at(0));
  EXPECT_EQ(2, bmm.sorted_iter_idxes_.at(1));
  EXPECT_EQ(0, bmm.sorted_iter_idxes_.at(2));

  // Refresh every score even when the index array is already permuted.
  bmm.dim_max_scores_.at(0) = -1.0;
  ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
  EXPECT_DOUBLE_EQ(3.0, bmm.dim_max_scores_.at(0));
  EXPECT_EQ(0, bmm.sorted_iter_idxes_.at(2));
  ASSERT_EQ(OB_SUCCESS, bmm.close());
  EXPECT_EQ(0, bmm.dim_max_scores_.count());
}

TEST_F(TestDASBMM, explicit_build_options_survive_config_change_and_rescan)
{
  const uint64_t ids[] = {1, 5, 9};
  const double scores[] = {2.0, 4.0, 8.0};
  const uint64_t filter_ids[] = {5};
  const double filter_scores[] = {0.0};
  for (int64_t mode = 0; mode < 2; ++mode) {
    const bool enabled = 0 != mode;
    MockBMMTokenOpParam token_param;
    MockBMMPostingOp token(search_ctx_, ids, scores, ARRAYSIZEOF(ids));
    MockBMMPostingOp ordered_filter(search_ctx_, filter_ids, filter_scores, 1);
    MockDirectBMMFilterOp direct_filter(search_ctx_, 5);
    ASSERT_EQ(OB_SUCCESS, token.init(token_param));
    ASSERT_EQ(OB_SUCCESS, ordered_filter.init(token_param));
    ASSERT_EQ(OB_SUCCESS, direct_filter.init(token_param));
    ObIDASSearchOp *filter = enabled
        ? static_cast<ObIDASSearchOp *>(&direct_filter) : &ordered_filter;
    ObSEArray<ObIDASSearchOp *, 1> score_ops;
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
    ObDASBMMBuildOptions options(enabled, enabled
        ? ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN
        : ObDASBMMBuildOptions::FilterMode::ORDERED);
    options.lead_cost_ = ObDASSearchCost(123);
    options.filter_cost_ = ObDASSearchCost(23);
    ObDASBMMOpParam param(score_ops, filter, allocator_, options);
    param.set_is_scoring(true);
    // The param owns a copy; neither the caller's value nor live config can
    // change the protocol selected when the filter/BMM pair was prepared.
    options.enable_vectorized_merge_ = !enabled;
    options.filter_mode_ = ObDASBMMBuildOptions::FilterMode::ORDERED;
    options.query_optional_ = true;
    options.lead_cost_.reset();
    options.filter_cost_.reset();
    BMMVectorizedMergeConfigGuard changed_config(!enabled);
    ASSERT_TRUE(changed_config.is_valid());
    ASSERT_NE(nullptr, param.get_build_options());
    EXPECT_EQ(enabled, param.get_build_options()->enable_vectorized_merge_);
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, bmm.init(param));
    EXPECT_EQ(enabled, bmm.enable_vec_merge_);
    EXPECT_FALSE(bmm.query_optional_);
    EXPECT_EQ(123, bmm.lead_cost_.cost());
    EXPECT_EQ(23, bmm.filter_cost_.cost());
    EXPECT_EQ(enabled ? ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN
        : ObDASBMMBuildOptions::FilterMode::ORDERED, bmm.filter_mode_);
    ASSERT_EQ(OB_SUCCESS, bmm.open());
    for (int64_t scan = 0; scan < 2; ++scan) {
      BMMVectorizedMergeConfigGuard scan_config(0 == scan ? !enabled : enabled);
      ASSERT_TRUE(scan_config.is_valid());
      if (scan > 0) {
        ASSERT_EQ(OB_SUCCESS, bmm.rescan());
      }
      EXPECT_EQ(enabled, bmm.enable_vec_merge_);
      EXPECT_EQ(enabled ? ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN
          : ObDASBMMBuildOptions::FilterMode::ORDERED, bmm.filter_mode_);
      ObDASRowID id;
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(id, score));
      EXPECT_EQ(5, id.get_uint64());
      EXPECT_DOUBLE_EQ(4.0, score);
      EXPECT_EQ(OB_ITER_END, bmm.next_rowid(id, score));
      if (enabled) {
        EXPECT_GT(direct_filter.get_drain_count(), 0);
      }
    }
    ASSERT_EQ(OB_SUCCESS, bmm.close());
  }
}

TEST_F(TestDASBMM, direct_capability_does_not_select_consumer_mode)
{
  class DirectCapablePostingOp : public MockBMMPostingOp
  {
  public:
    using MockBMMPostingOp::MockBMMPostingOp;
    bool supports_direct_rowid_drain() const override { return true; }
    // The inherited direct drain rejects calls, detecting accidental selection
    // of this protocol by an ordered consumer.
  };
  const uint64_t ids[] = {1, 5, 9};
  const double scores[] = {2.0, 4.0, 8.0};
  for (int64_t mode = 0; mode < 2; ++mode) {
    for (int64_t optional = 0; optional < 2; ++optional) {
      SCOPED_TRACE(::testing::Message() << "mode=" << mode << " optional=" << optional);
      const uint64_t filter_ids[] = {5};
      const double filter_scores[] = {0.0};
      MockBMMTokenOpParam param;
      MockBMMPostingOp token(search_ctx_, ids, scores, ARRAYSIZEOF(ids));
      DirectCapablePostingOp filter(search_ctx_, filter_ids, filter_scores, 1);
      ASSERT_EQ(OB_SUCCESS, token.init(param));
      ASSERT_EQ(OB_SUCCESS, filter.init(param));
      ObSEArray<ObIDASSearchOp *, 1> score_ops;
      ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
      ObDASBMMBuildOptions bmm_param_options(0 != mode);
      bmm_param_options.query_optional_ = 0 != optional;
      ObDASBMMOpParam bmm_param(score_ops, &filter, allocator_, bmm_param_options);
      bmm_param.set_is_scoring(true);
      ASSERT_TRUE(bmm_param.is_valid());
      ObDASBMMOp bmm(search_ctx_);
      ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
      EXPECT_EQ(0 != mode, bmm.enable_vec_merge_);
      EXPECT_EQ(ObDASBMMBuildOptions::FilterMode::ORDERED, bmm.filter_mode_);
      ASSERT_EQ(OB_SUCCESS, bmm.open());
      for (int64_t scan = 0; scan < 2; ++scan) {
        if (scan > 0) {
          ASSERT_EQ(OB_SUCCESS, bmm.rescan());
        }
        ObDASRowID rowid;
        double score = 0.0;
        ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
        EXPECT_EQ(5, rowid.get_uint64());
        EXPECT_DOUBLE_EQ(4.0, score);
        EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
      }
      ASSERT_EQ(OB_SUCCESS, bmm.close());
    }
  }
}

TEST_F(TestDASBMM, ordinary_init_reads_live_config_per_instance)
{
  for (int64_t with_filter = 0; with_filter < 2; ++with_filter) {
    for (int64_t mode = 0; mode < 2; ++mode) {
      BMMVectorizedMergeConfigGuard config_guard(0 != mode);
      ASSERT_TRUE(config_guard.is_valid());
      MockBMMTokenOpParam token_param;
      MockBMMTokenOp token(search_ctx_, 5, 9, 4.0, 4.0);
      MockBMMTokenOp filter(search_ctx_, 5, 9, 0.0, 0.0);
      ASSERT_EQ(OB_SUCCESS, token.init(token_param));
      ASSERT_EQ(OB_SUCCESS, filter.init(token_param));
      ObSEArray<ObIDASSearchOp *, 1> score_ops;
      ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
      ObDASBMMBuildOptions options;
      options.lead_cost_ = ObDASSearchCost(1001);
      options.filter_cost_ = ObDASSearchCost(10);
      ObDASBMMOpParam param(
          score_ops, 0 != with_filter ? &filter : nullptr, allocator_, options);
      param.set_is_scoring(true);
      EXPECT_EQ(nullptr, param.get_build_options());
      ObDASBMMOp bmm(search_ctx_);
      ASSERT_EQ(OB_SUCCESS, bmm.init(param));
      EXPECT_EQ(0 != mode, bmm.enable_vec_merge_);
      EXPECT_EQ(1001, bmm.lead_cost_.cost());
      EXPECT_EQ(10, bmm.filter_cost_.cost());
      BMMVectorizedMergeConfigGuard changed_config(0 == mode);
      ASSERT_TRUE(changed_config.is_valid());
      ASSERT_EQ(OB_SUCCESS, bmm.open());
      for (int64_t scan = 0; scan < 2; ++scan) {
        if (scan > 0) {
          ASSERT_EQ(OB_SUCCESS, bmm.rescan());
        }
        EXPECT_EQ(0 != mode, bmm.enable_vec_merge_);
        ObDASRowID id;
        double score = 0.0;
        ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(id, score));
        EXPECT_EQ(5, id.get_uint64());
        EXPECT_DOUBLE_EQ(4.0, score);
        EXPECT_EQ(OB_ITER_END, bmm.next_rowid(id, score));
      }
      ASSERT_EQ(OB_SUCCESS, bmm.close());
    }
  }
}

TEST_F(TestDASBMM, direct_filter_requires_explicit_compatible_build_options)
{
  MockBMMTokenOpParam token_param;
  MockBMMTokenOp token(search_ctx_, 5, 9, 4.0, 4.0);
  MockDirectBMMFilterOp filter(search_ctx_, 5);
  ASSERT_EQ(OB_SUCCESS, token.init(token_param));
  ASSERT_EQ(OB_SUCCESS, filter.init(token_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
  for (int64_t mode = 0; mode < 2; ++mode) {
    BMMVectorizedMergeConfigGuard config_guard(0 != mode);
    ASSERT_TRUE(config_guard.is_valid());
    ObDASBMMOpParam missing_options(score_ops, &filter, allocator_, ObDASBMMBuildOptions());
    ObDASBMMOpParam disabled_options(score_ops, &filter, allocator_, ObDASBMMBuildOptions(false));
    ObDASBMMOpParam ordered_options(score_ops, &filter, allocator_, ObDASBMMBuildOptions(true));
    ObDASBMMOp first_bmm(search_ctx_);
    ObDASBMMOp second_bmm(search_ctx_);
    EXPECT_FALSE(missing_options.is_valid());
    EXPECT_FALSE(disabled_options.is_valid());
    EXPECT_FALSE(ordered_options.is_valid());
    EXPECT_EQ(OB_INVALID_ARGUMENT, first_bmm.init(missing_options));
    EXPECT_EQ(OB_INVALID_ARGUMENT, second_bmm.init(disabled_options));
  }

  ObDASBMMOp optional_bmm(search_ctx_);
  ObDASBMMBuildOptions optional_param_options(
      true, ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN);
  optional_param_options.query_optional_ = true;
  ObDASBMMOpParam optional_param(score_ops, &filter, allocator_, optional_param_options);
  EXPECT_EQ(OB_NOT_SUPPORTED, optional_bmm.init_vec_merge_on_demand(optional_param, allocator_));
  search_ctx_.get_logical_table_ctx().is_enabled_ = true;
  ObDASBMMOp logical_bmm(search_ctx_);
  ObDASBMMOpParam logical_param(
      score_ops, &filter, allocator_,
      ObDASBMMBuildOptions(true, ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN));
  EXPECT_EQ(OB_NOT_SUPPORTED, logical_bmm.init_vec_merge_on_demand(logical_param, allocator_));
  search_ctx_.get_logical_table_ctx().is_enabled_ = false;
}

enum class GateFilterKind
{
  NONE,
  ENUMERABLE,
  NON_ENUMERABLE,
};

struct VectorizedGateCase
{
  bool config_enabled_;
  ObDASRowIDType rowid_type_;
  bool logical_table_;
  bool query_optional_;
  GateFilterKind filter_kind_;
  bool expected_enabled_;
};

class TestDASBMMVectorizedGate
  : public TestDASBMM,
    public ::testing::WithParamInterface<VectorizedGateCase>
{};

// Cover vectorized BMM enablement across configuration, RowID type, logical-table,
// optional-query, and filter-enumerability combinations, checking the decision and that
// vector-only buffers are allocated exclusively when the gate succeeds.
TEST_P(TestDASBMMVectorizedGate, vectorized_enablement_gate_matrix)
{
  const VectorizedGateCase &test_case = GetParam();
  BMMVectorizedMergeConfigGuard config_guard(test_case.config_enabled_);
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(5, 1.0, 100, 1.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, postings, 1.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, postings, 0.0);
  NonEnumerableScriptedBMMSearchOp non_enumerable_filter(search_ctx_, postings, 0.0);
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, score_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ASSERT_EQ(OB_SUCCESS, non_enumerable_filter.init(filter_param));
  ObIDASSearchOp *selected_filter = nullptr;
  if (GateFilterKind::ENUMERABLE == test_case.filter_kind_) {
    selected_filter = &filter_op;
  } else if (GateFilterKind::NON_ENUMERABLE == test_case.filter_kind_) {
    selected_filter = &non_enumerable_filter;
  }
  search_ctx_.rowid_type_ = test_case.rowid_type_;
  search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_type_ = test_case.rowid_type_;
  search_ctx_.logical_table_ctx_.is_enabled_ = test_case.logical_table_;

  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      bmm, score_ops, selected_filter, 100000, 1001, test_case.query_optional_));
  EXPECT_EQ(test_case.expected_enabled_, bmm.enable_vec_merge_);
  if (test_case.expected_enabled_) {
    EXPECT_TRUE(bmm.doc_score_accumulator_.is_valid());
    EXPECT_EQ(nullptr != selected_filter, nullptr != bmm.filter_bitset_);
    EXPECT_EQ(0, bmm.sparse_candidate_buffer_.get_capacity());
  } else {
    EXPECT_FALSE(bmm.doc_score_accumulator_.is_valid());
    EXPECT_EQ(nullptr, bmm.filter_bitset_);
    EXPECT_EQ(0, bmm.sparse_candidate_buffer_.get_capacity());
  }
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

INSTANTIATE_TEST_CASE_P(
    VectorizedGateMatrix,
    TestDASBMMVectorizedGate,
    ::testing::Values(
        VectorizedGateCase{true, DAS_ROWID_TYPE_UINT64, false, false,
            GateFilterKind::NONE, true},
        VectorizedGateCase{false, DAS_ROWID_TYPE_UINT64, false, false,
            GateFilterKind::NONE, false},
        VectorizedGateCase{true, DAS_ROWID_TYPE_COMPACT, false, false,
            GateFilterKind::NONE, false},
        VectorizedGateCase{true, DAS_ROWID_TYPE_UINT64, true, false,
            GateFilterKind::NONE, false},
        VectorizedGateCase{true, DAS_ROWID_TYPE_UINT64, false, true,
            GateFilterKind::NONE, false},
        VectorizedGateCase{true, DAS_ROWID_TYPE_UINT64, false, true,
            GateFilterKind::ENUMERABLE, true},
        VectorizedGateCase{true, DAS_ROWID_TYPE_UINT64, false, true,
            GateFilterKind::NON_ENUMERABLE, false},
        VectorizedGateCase{true, DAS_ROWID_TYPE_UINT64, false, false,
            GateFilterKind::NON_ENUMERABLE, true}));

struct FilterModeCase
{
  int64_t table_row_count_;
  int64_t lead_cost_;
  bool has_filter_;
  ObDASBMMOp::FilterMergeMode expected_mode_;
};

class TestDASBMMFilterMode
  : public TestDASBMM,
    public ::testing::WithParamInterface<FilterModeCase>
{};

// Exercise dense-versus-sparse filter-mode selection at invalid and exact cost-ratio boundaries,
// ensuring each mode allocates only its required accumulator, filter bitset, or candidate buffer.
TEST_P(TestDASBMMFilterMode, filter_merge_mode_cost_boundary_matrix)
{
  const FilterModeCase &test_case = GetParam();
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPostingList postings = ScriptedBMMPostingBuilder()
      .add(5, 1.0, 100, 1.0)
      .build();
  ScriptedBMMSearchOp score_op(search_ctx_, postings, 1.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, postings, 0.0);
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, score_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObIDASSearchOp *selected_filter = test_case.has_filter_ ? &filter_op : nullptr;

  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, selected_filter,
      test_case.table_row_count_, test_case.lead_cost_));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  EXPECT_EQ(test_case.expected_mode_, bmm.filter_merge_mode_);
  if (ObDASBMMOp::FilterMergeMode::SPARSE_SEEK == test_case.expected_mode_) {
    EXPECT_EQ(ObDAATMergeIter::DRAIN_BATCH_SIZE,
        bmm.sparse_candidate_buffer_.get_capacity());
    EXPECT_FALSE(bmm.doc_score_accumulator_.is_valid());
    EXPECT_EQ(nullptr, bmm.filter_bitset_);
  } else {
    EXPECT_EQ(0, bmm.sparse_candidate_buffer_.get_capacity());
    EXPECT_TRUE(bmm.doc_score_accumulator_.is_valid());
    EXPECT_EQ(test_case.has_filter_, nullptr != bmm.filter_bitset_);
  }
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

INSTANTIATE_TEST_CASE_P(
    FilterModeCostMatrix,
    TestDASBMMFilterMode,
    ::testing::Values(
        FilterModeCase{-1, 1, true, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW},
        FilterModeCase{1000, -1, true, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW},
        FilterModeCase{99, 1, true, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW},
        FilterModeCase{100, 1, true, ObDASBMMOp::FilterMergeMode::SPARSE_SEEK},
        FilterModeCase{100, 2, true, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW},
        FilterModeCase{1000000, 10000, true, ObDASBMMOp::FilterMergeMode::SPARSE_SEEK},
        FilterModeCase{1000000, 10001, true, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW},
        FilterModeCase{100, 1, false, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW}));

// Validate the independent BMM oracle by summing overlapping dimensions, intersecting a filter,
// applying a strict score threshold, and excluding scoring-only and filter-only documents.
TEST(BMMReferenceOracleTest, aggregates_filters_and_applies_strict_threshold)
{
  const ScriptedBMMPostingList dim_a = ScriptedBMMPostingBuilder()
      .add(1, 1.5, 100, 4.0)
      .add(3, 2.0, 100, 4.0)
      .add(7, 4.0, 100, 4.0)
      .add(9, 4.0, 100, 4.0)
      .build();
  const ScriptedBMMPostingList dim_b = ScriptedBMMPostingBuilder()
      .add(3, 3.0, 100, 6.0)
      .add(5, 6.0, 100, 6.0)
      .add(7, 1.0, 100, 6.0)
      .build();
  const ScriptedBMMPostingList filter = ScriptedBMMPostingBuilder()
      .add(3, 0.0, 100, 0.0)
      .add(5, 0.0, 100, 0.0)
      .add(7, 0.0, 100, 0.0)
      .add(9, 0.0, 100, 0.0)
      .add(11, 0.0, 100, 0.0)
      .build();
  const std::vector<ScriptedBMMPostingList> scoring_postings = {dim_a, dim_b};

  const std::vector<BMMReferenceResult> results =
      BMMReferenceOracle::evaluate(scoring_postings, &filter, 4.0);

  ASSERT_EQ(3, results.size());
  EXPECT_EQ(3, results[0].doc_id_);
  EXPECT_DOUBLE_EQ(5.0, results[0].score_);
  EXPECT_EQ(5, results[1].doc_id_);
  EXPECT_DOUBLE_EQ(6.0, results[1].score_);
  EXPECT_EQ(7, results[2].doc_id_);
  EXPECT_DOUBLE_EQ(5.0, results[2].score_);
}

TEST_F(TestDASBMM, dim_max_score_promotes_token_and_expands_boundary)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  ScriptedBMMSearchOpParam token_param;
  // A 的当前 block 最早结束，全局上界为 6，最终保留为唯一的 non-essential token。
  ScriptedBMMSearchOp token_a(search_ctx_, 5, 20, 2.0, 6.0);
  // B 的当前 block 上界不会使旧前缀越过 10，但全局上界会将它提升为 essential。
  ScriptedBMMSearchOp token_b(search_ctx_, 7, 100, 3.0, 7.0);
  // C 是原始 essential token，并提供最远的当前 block 终点。
  ScriptedBMMSearchOp token_c(search_ctx_, 9, 1000, 8.0, 8.0);
  ASSERT_EQ(OB_SUCCESS, token_a.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_b.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_c.init(token_param));

  // 将三个 mock token 安装到真实 BMM 算子中。
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_b));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_c));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, nullptr, 100000, 1000));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
  bmm.min_competitive_score_ = 10.0;

  // 从 doc_id 1 开始计算第一个向量化 boundary。
  ObDASRowID target;
  target.set_uint64(1);
  ASSERT_EQ(OB_SUCCESS, bmm.compute_vec_boundary_at(target));

  // 校验全局前缀最终保留 A，并将 B、C 划入 essential 后缀。
  EXPECT_EQ(1, bmm.non_essential_dim_count_);
  EXPECT_DOUBLE_EQ(6.0, bmm.non_essential_dim_max_score_);
  EXPECT_DOUBLE_EQ(13.0, bmm.non_essential_dim_threshold_);
  // The natural boundary is 100 and the minimum vectorized window expands it to 16K.
  EXPECT_EQ(4096 * 4, bmm.vec_boundary_id_.get_uint64());
  // 校验 non-essential 剪枝保存 A 的全局上界 6，而不是当前 block 上界 2。
  EXPECT_DOUBLE_EQ(6.0, bmm.non_ess_block_max_score_suffix_sums_.at(0));

  // 校验重建后的 essential merger 从 B 开始，不包含 non-essential token A。
  ObDASRowID top_id;
  ASSERT_EQ(OB_SUCCESS, bmm.row_merger_->get_top_id(top_id));
  EXPECT_EQ(7, top_id.get_uint64());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, expand_vec_boundary_handles_limits)
{
  ObDASBMMOp bmm(search_ctx_);
  ObDASRowID candidate_start;
  ObDASRowID boundary;

  // Expand a former 4K boundary to the new 16K minimum.
  candidate_start.set_uint64(1);
  boundary.set_uint64(4096);
  ASSERT_EQ(OB_SUCCESS, bmm.expand_vec_boundary(candidate_start, boundary));
  EXPECT_EQ(4096 * 4, boundary.get_uint64());

  // Preserve a natural boundary that already exceeds the minimum window.
  boundary.set_uint64(20000);
  ASSERT_EQ(OB_SUCCESS, bmm.expand_vec_boundary(candidate_start, boundary));
  EXPECT_EQ(20000, boundary.get_uint64());

  // 将扩展后的最小右边界截断到有限查询范围内。
  ObObj range_lo;
  ObObj range_hi;
  range_lo.set_uint64(0);
  range_hi.set_uint64(500);
  search_ctx_.set_docid_range(range_lo, range_hi);
  boundary.set_uint64(100);
  ASSERT_EQ(OB_SUCCESS, bmm.expand_vec_boundary(candidate_start, boundary));
  EXPECT_EQ(500, boundary.get_uint64());

  // 接近 UINT64_MAX 时将闭区间右边界饱和到最大值，避免回绕。
  search_ctx_.has_docid_range_ = false;
  candidate_start.set_uint64(UINT64_MAX - 100);
  boundary.set_uint64(UINT64_MAX - 50);
  ASSERT_EQ(OB_SUCCESS, bmm.expand_vec_boundary(candidate_start, boundary));
  EXPECT_EQ(UINT64_MAX, boundary.get_uint64());

  // 保持 MAX 哨兵不变。
  boundary.set_max();
  ASSERT_EQ(OB_SUCCESS, bmm.expand_vec_boundary(candidate_start, boundary));
  EXPECT_TRUE(boundary.is_max());
}

TEST_F(TestDASBMM, accumulator_retain_by_mask_preserves_scores_and_state)
{
  DocScoreAccumulator accumulator;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(8, allocator_));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(10, max_id));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(10, 1.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(12, 7.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(15, 3.0));
  EXPECT_EQ(3, accumulator.get_active_count());

  void *mask_buf = allocator_.alloc(ObBitVector::memory_size(8));
  ASSERT_NE(nullptr, mask_buf);
  ObBitVector *mask = to_bit_vector(mask_buf);
  mask->init(8);
  mask->reset(8);
  mask->set(0);
  mask->set(5);
  ASSERT_EQ(OB_SUCCESS, accumulator.retain_by_mask(*mask));
  EXPECT_FALSE(accumulator.is_empty());
  EXPECT_DOUBLE_EQ(3.0, accumulator.get_max_score());
  EXPECT_EQ(2, accumulator.get_active_count());

  DocScoreIter iter;
  uint64_t rowid = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(10, rowid);
  EXPECT_DOUBLE_EQ(1.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(15, rowid);
  EXPECT_DOUBLE_EQ(3.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));

  mask->reset(8);
  ASSERT_EQ(OB_SUCCESS, accumulator.retain_by_mask(*mask));
  EXPECT_TRUE(accumulator.is_empty());
  EXPECT_DOUBLE_EQ(0.0, accumulator.get_max_score());
  EXPECT_EQ(0, accumulator.get_active_count());
}

TEST_F(TestDASBMM, dense_filter_strategy_uses_filter_cost_ratio)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  MockBMMTokenOpParam score_param;
  MockBMMTokenOpParam filter_param;
  MockBMMTokenOp score_op(search_ctx_, 5, 100, 5.0, 5.0);
  MockBMMTokenOp filter_op(search_ctx_, 5, 100, 0.0, 0.0);
  ASSERT_EQ(OB_SUCCESS, score_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp low_cost_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(low_cost_bmm, score_ops, &filter_op, 1000, 11, false, 61));
  EXPECT_FALSE(low_cost_bmm.filter_after_score_);
  EXPECT_FALSE(low_cost_bmm.use_active_candidate_probe_);
  EXPECT_EQ(OB_SUCCESS, low_cost_bmm.close());

  ObDASBMMOp high_cost_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(high_cost_bmm, score_ops, &filter_op, 1000, 11, false, 62));
  EXPECT_TRUE(high_cost_bmm.filter_after_score_);
  EXPECT_TRUE(high_cost_bmm.use_active_candidate_probe_);
  ASSERT_EQ(OB_SUCCESS, high_cost_bmm.rescan());
  EXPECT_TRUE(high_cost_bmm.filter_after_score_);
  EXPECT_TRUE(high_cost_bmm.use_active_candidate_probe_);
  EXPECT_EQ(OB_SUCCESS, high_cost_bmm.close());

  ObDASBMMOp very_dense_filter_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(very_dense_filter_bmm, score_ops, &filter_op, 1024, 11, false, 512));
  EXPECT_TRUE(very_dense_filter_bmm.filter_after_score_);
  EXPECT_FALSE(very_dense_filter_bmm.use_active_candidate_probe_);
  EXPECT_EQ(OB_SUCCESS, very_dense_filter_bmm.close());

  ObDASBMMOp invalid_cost_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(invalid_cost_bmm, score_ops, &filter_op, 1000, 11, false, -1));
  EXPECT_FALSE(invalid_cost_bmm.filter_after_score_);
  EXPECT_FALSE(invalid_cost_bmm.use_active_candidate_probe_);
  EXPECT_EQ(OB_SUCCESS, invalid_cost_bmm.close());

  ObDASBMMOp optional_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(optional_bmm, score_ops, &filter_op, 1000, 11, true, 500));
  EXPECT_FALSE(optional_bmm.filter_after_score_);
  EXPECT_FALSE(optional_bmm.use_active_candidate_probe_);
  EXPECT_EQ(OB_SUCCESS, optional_bmm.close());

  ObDASBMMOp sparse_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(sparse_bmm, score_ops, &filter_op, 1000, 10, false, 500));
  EXPECT_EQ(ObDASBMMOp::FilterMergeMode::SPARSE_SEEK, sparse_bmm.filter_merge_mode_);
  EXPECT_FALSE(sparse_bmm.filter_after_score_);
  EXPECT_FALSE(sparse_bmm.use_active_candidate_probe_);
  EXPECT_EQ(OB_SUCCESS, sparse_bmm.close());

  ObDASBMMOp no_filter_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(no_filter_bmm, score_ops, nullptr, 1000, 11, false, 500));
  EXPECT_FALSE(no_filter_bmm.filter_after_score_);
  EXPECT_TRUE(no_filter_bmm.use_active_candidate_probe_);
  ASSERT_EQ(OB_SUCCESS, no_filter_bmm.rescan());
  EXPECT_TRUE(no_filter_bmm.use_active_candidate_probe_);
  EXPECT_EQ(OB_SUCCESS, no_filter_bmm.close());
}

TEST_F(TestDASBMM, direct_build_decision_survives_cost_changes)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  struct CostCase {
    int64_t table_row_count_;
    int64_t lead_cost_;
    int64_t filter_cost_;
  };
  const CostCase cases[] = {
    {1000, 20, 1},
    {1000, 10, 1},
    {1000, 20, 900},
    {10000, 20, 1},
    {16, 20, 1},
  };
  const uint64_t score_ids[] = {5, 6, 7};
  const double score_values[] = {4.0, 3.0, 2.0};
  for (const CostCase &test_case : cases) {
    SCOPED_TRACE(::testing::Message() << "rows=" << test_case.table_row_count_
        << " lead=" << test_case.lead_cost_ << " filter=" << test_case.filter_cost_);
    search_ctx_.table_row_count_ = ObDASSearchCost(1000);
    ObDASBMMBuildOptions options(false);
    ObDASBMMOp::select_filter_build_options(search_ctx_, ObDASSearchCost(20),
        ObDASSearchCost(1), false, true, options);
    ASSERT_EQ(ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN, options.filter_mode_);

    MockBMMTokenOpParam op_param;
    MockBMMPostingOp score_op(
        search_ctx_, score_ids, score_values, ARRAYSIZEOF(score_ids));
    MockDirectBMMFilterOp filter_op(search_ctx_, 5);
    ASSERT_EQ(OB_SUCCESS, score_op.init(op_param));
    ASSERT_EQ(OB_SUCCESS, filter_op.init(op_param));
    ObSEArray<ObIDASSearchOp *, 1> score_ops;
    ObSEArray<ObDASSearchCost, 1> score_op_costs;
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
    ASSERT_EQ(OB_SUCCESS, score_op_costs.push_back(ObDASSearchCost(3)));
    search_ctx_.table_row_count_ = ObDASSearchCost(test_case.table_row_count_);
    ObDASBMMBuildOptions bmm_param_options = options;
    bmm_param_options.query_optional_ = false;
    bmm_param_options.lead_cost_ = ObDASSearchCost(test_case.lead_cost_);
    bmm_param_options.filter_cost_ = ObDASSearchCost(test_case.filter_cost_);
    ObDASBMMOpParam bmm_param(score_ops, &filter_op, allocator_, bmm_param_options, &score_op_costs);
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
    ASSERT_EQ(OB_SUCCESS, bmm.open());
    for (int64_t scan = 0; scan < 2; ++scan) {
      if (scan > 0) {
        ASSERT_EQ(OB_SUCCESS, bmm.rescan());
      }
      EXPECT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
      EXPECT_FALSE(bmm.filter_after_score_);
      ObDASRowID rowid;
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
      EXPECT_EQ(5, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(4.0, score);
      EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
      EXPECT_GT(filter_op.get_drain_count(), 0);
    }
    ASSERT_EQ(OB_SUCCESS, bmm.close());
  }
}

TEST_F(TestDASBMM, direct_capability_preserves_filter_strategy_and_results)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  struct StrategyCase {
    int64_t lead_cost_;
    int64_t filter_cost_;
    bool query_optional_;
    ObDASBMMOp::FilterMergeMode merge_mode_;
    bool filter_after_score_;
    bool allow_direct_;
  };
  const StrategyCase cases[] = {
    {20, 61, false, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, false, true},
    {20, 62, false, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, true, false},
    {20, 900, false, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, true, false},
    {10, 61, false, ObDASBMMOp::FilterMergeMode::SPARSE_SEEK, false, false},
    {10, 900, false, ObDASBMMOp::FilterMergeMode::SPARSE_SEEK, false, false},
    {20, 61, true, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, false, false},
    {-1, -1, false, ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, false, true},
  };
  const uint64_t score_ids[] = {5, 6, 7};
  const double score_values[] = {4.0, 3.0, 2.0};
  const uint64_t filter_ids[] = {5};
  const double filter_scores[] = {0.0};
  search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  for (const StrategyCase &test_case : cases) {
    for (int64_t capability = 0; capability < 2; ++capability) {
      SCOPED_TRACE(::testing::Message() << "lead=" << test_case.lead_cost_
          << " filter=" << test_case.filter_cost_
          << " optional=" << test_case.query_optional_ << " direct_capability=" << capability);
      const bool use_direct = test_case.allow_direct_ && 0 != capability;
      ObDASBMMBuildOptions options(false);
      ObDASBMMOp::select_filter_build_options(search_ctx_,
          ObDASSearchCost(test_case.lead_cost_), ObDASSearchCost(test_case.filter_cost_),
          test_case.query_optional_, 0 != capability, options);
      EXPECT_EQ(use_direct ? ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN
          : ObDASBMMBuildOptions::FilterMode::ORDERED, options.filter_mode_);
      MockBMMTokenOpParam op_param;
      MockBMMPostingOp token(search_ctx_, score_ids, score_values, ARRAYSIZEOF(score_ids));
      MockBMMPostingOp ordered_filter(search_ctx_, filter_ids, filter_scores, 1);
      MockDirectBMMFilterOp direct_filter(search_ctx_, 5);
      ASSERT_EQ(OB_SUCCESS, token.init(op_param));
      ASSERT_EQ(OB_SUCCESS, ordered_filter.init(op_param));
      ASSERT_EQ(OB_SUCCESS, direct_filter.init(op_param));
      ObIDASSearchOp *filter = use_direct
          ? static_cast<ObIDASSearchOp *>(&direct_filter) : &ordered_filter;
      ObSEArray<ObIDASSearchOp *, 1> score_ops;
      ObSEArray<ObDASSearchCost, 1> score_costs;
      ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
      ASSERT_EQ(OB_SUCCESS, score_costs.push_back(ObDASSearchCost(3)));
      ObDASBMMBuildOptions bmm_param_options = options;
      bmm_param_options.query_optional_ = test_case.query_optional_;
      bmm_param_options.lead_cost_ = ObDASSearchCost(test_case.lead_cost_);
      bmm_param_options.filter_cost_ = ObDASSearchCost(test_case.filter_cost_);
      ObDASBMMOpParam bmm_param(score_ops, filter, allocator_, bmm_param_options, &score_costs);
      ObDASBMMOp bmm(search_ctx_);
      ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
      ASSERT_EQ(OB_SUCCESS, bmm.open());
      for (int64_t scan = 0; scan < 2; ++scan) {
        if (scan > 0) {
          ASSERT_EQ(OB_SUCCESS, bmm.rescan());
        }
        EXPECT_EQ(test_case.merge_mode_, bmm.filter_merge_mode_);
        EXPECT_EQ(test_case.filter_after_score_, bmm.filter_after_score_);
        ObDASRowID rowid;
        double score = 0.0;
        ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
        EXPECT_EQ(5, rowid.get_uint64());
        EXPECT_DOUBLE_EQ(4.0, score);
        EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
      }
      EXPECT_EQ(use_direct, direct_filter.get_drain_count() > 0);
      ASSERT_EQ(OB_SUCCESS, bmm.close());
    }
  }
}

TEST_F(TestDASBMM, no_filter_non_essential_probe_requires_dense_posting_cost)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const MockBMMPosting postings[] = {
      MockBMMPosting(10, 1.0, 256, 1.0),
      MockBMMPosting(12, 1.0, 256, 1.0),
      MockBMMPosting(20, 1.0, 256, 1.0),
      MockBMMPosting(30, 1.0, 256, 1.0)};
  struct ProbeCase
  {
    int64_t candidate_count_;
    int64_t posting_cost_;
    int64_t table_row_count_;
    bool expect_probe_;
  };
  const ProbeCase cases[] = {
      {2, 64, 512, true}, {64, 64, 512, true}, {65, 64, 512, false},
      {2, 63, 512, false}, {2, 0, 512, false}, {2, -1, 512, false},
      {2, 64, -1, false}, {2, 1, 7, false}};
  for (const ProbeCase &probe_case : cases) {
    const int64_t candidate_count = probe_case.candidate_count_;
    MockBMMTokenOpParam token_param;
    token_param.set_is_scoring(true);
    MockBMMTokenOp token(search_ctx_, postings, ARRAYSIZEOF(postings), 1.0);
    token.enable_advance_to_id();
    ASSERT_EQ(OB_SUCCESS, token.init(token_param));
    ObSEArray<ObIDASSearchOp *, 1> score_ops;
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS,
        init_bmm_with_costs(bmm, score_ops, nullptr, probe_case.table_row_count_, 11));
    bmm.score_op_costs_.at(0) = ObDASSearchCost(probe_case.posting_cost_);
    ASSERT_EQ(OB_SUCCESS, bmm.open());
    for (int64_t round = 0; round < 2; ++round) {
      if (round > 0) {
        ASSERT_EQ(OB_SUCCESS, bmm.rescan());
      }
      ASSERT_TRUE(bmm.use_active_candidate_probe_);
      ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
      bmm.vec_non_essential_dim_count_ = 1;
      bmm.vec_sorted_iter_idxes_.at(0) = 0;
      bmm.non_ess_block_max_score_suffix_sums_.at(0) = 1.0;
      bmm.min_competitive_score_ = 5.5;
      uint64_t max_id = 0;
      ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.reuse_with_start_id(10, max_id));
      for (int64_t i = 0; i < candidate_count; ++i) {
        ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(10 + i * 2, 5.0));
      }
      ObDASRowID boundary;
      boundary.set_uint64(256);
      bool valid = false;
      const int64_t old_probes = token.get_advance_to_id_count();
      ASSERT_EQ(OB_SUCCESS, bmm.evaluate_non_essential_batch(boundary, valid));
      EXPECT_TRUE(valid);
      EXPECT_EQ(candidate_count == 2 ? 2 : 4, bmm.doc_score_accumulator_.get_active_count());
      if (probe_case.expect_probe_) {
        EXPECT_GT(token.get_advance_to_id_count(), old_probes);
      } else {
        EXPECT_EQ(old_probes, token.get_advance_to_id_count());
      }
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.get_score(10, score));
      EXPECT_DOUBLE_EQ(6.0, score);
    }
    EXPECT_EQ(OB_SUCCESS, bmm.close());
  }
}

TEST_F(TestDASBMM, sparse_active_candidates_seek_and_score_exact_hits)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const MockBMMPosting postings[] = {
      MockBMMPosting(10, 1.0, 64, 4.0),
      MockBMMPosting(12, 2.0, 64, 4.0),
      MockBMMPosting(20, 3.0, 64, 4.0),
      MockBMMPosting(30, 4.0, 64, 4.0)};
  MockBMMTokenOpParam token_param;
  token_param.set_is_scoring(true);
  MockBMMTokenOp token(search_ctx_, postings, ARRAYSIZEOF(postings), 4.0);
  token.enable_advance_to_id();
  ASSERT_EQ(OB_SUCCESS, token.init(token_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(bmm, score_ops, nullptr, 1000, 11));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.reuse_with_start_id(10, max_id));
  for (const uint64_t doc_id : {10ULL, 11ULL, 12ULL, 25ULL, 40ULL}) {
    ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(doc_id, 5.0));
  }
  ASSERT_EQ(OB_SUCCESS, bmm.score_active_candidates(*bmm.merge_iters_.at(0)));

  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.get_score(10, score));
  EXPECT_DOUBLE_EQ(6.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.get_score(11, score));
  EXPECT_DOUBLE_EQ(5.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.get_score(12, score));
  EXPECT_DOUBLE_EQ(7.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.get_score(25, score));
  EXPECT_DOUBLE_EQ(5.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.get_score(40, score));
  EXPECT_DOUBLE_EQ(5.0, score);
  EXPECT_EQ(5, token.get_advance_to_id_count());
  EXPECT_EQ(2, token.get_load_curr_score_count());
  EXPECT_TRUE(bmm.merge_iters_.at(0)->iter_end());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, candidate_filter_retains_only_scored_intersections)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const uint64_t filter_ids[] = {10, 12, 15, 18};
  const double filter_scores[] = {0.0, 0.0, 0.0, 0.0};
  MockBMMTokenOpParam score_param;
  MockBMMTokenOpParam filter_param;
  MockBMMTokenOp score_op(search_ctx_, 10, 100, 5.0, 5.0);
  MockBMMPostingOp filter_op(
      search_ctx_, filter_ids, filter_scores, ARRAYSIZEOF(filter_ids));
  ASSERT_EQ(OB_SUCCESS, score_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, false, 500));
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.reuse_with_start_id(10, max_id));
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(10, 1.0));
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(11, 2.0));
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(15, 3.0));
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(17, 4.0));
  ASSERT_EQ(OB_SUCCESS, bmm.apply_filter_to_scored_batch());

  DocScoreIter iter;
  uint64_t rowid = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, iter.open(bmm.doc_score_accumulator_));
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(10, rowid);
  EXPECT_DOUBLE_EQ(1.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(15, rowid);
  EXPECT_DOUBLE_EQ(3.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));
  EXPECT_DOUBLE_EQ(3.0, bmm.doc_score_accumulator_.get_max_score());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, dense_required_filter_cost_strategy_preserves_results)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  eval_ctx_.max_batch_size_ = 1;
  const uint64_t scoring_ids[] = {1, 2, 5, 16, 17, 18, 21, 31, 32, 33, 40};
  const double scoring_scores[] = {1.0, 2.0, 5.0, 16.0, 17.0, 18.0, 21.0, 31.0, 32.0, 33.0, 40.0};
  const uint64_t filter_ids[] = {
      2, 4, 6, 8, 10, 12, 14, 16, 18, 20,
      22, 24, 26, 28, 30, 32, 34, 36, 38, 40};
  const double filter_scores[ARRAYSIZEOF(filter_ids)] = {};
  MockBMMTokenOpParam score_param;
  MockBMMTokenOpParam filter_param;
  score_param.set_is_scoring(true);
  MockBMMPostingOp scoring_op(
      search_ctx_, scoring_ids, scoring_scores, ARRAYSIZEOF(scoring_ids));
  MockBMMPostingOp filter_op(
      search_ctx_, filter_ids, filter_scores, ARRAYSIZEOF(filter_ids));
  ASSERT_EQ(OB_SUCCESS, scoring_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, false, 500));
  ASSERT_EQ(64, bmm.doc_score_accumulator_.get_capacity());
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  const uint64_t expected_ids[] = {2, 16, 18, 32, 40};
  const double expected_scores[] = {2.0, 16.0, 18.0, 32.0, 40.0};
  ObDASRowID rowid;
  double score = 0.0;
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
    ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
    EXPECT_EQ(expected_ids[i], rowid.get_uint64());
    EXPECT_DOUBLE_EQ(expected_scores[i], score);
  }
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  EXPECT_TRUE(bmm.filter_after_score_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, candidate_filter_skips_noncompetitive_batches)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  MockBMMTokenOpParam param;
  MockBMMTokenOp score_op(search_ctx_, 10, 100, 5.0, 5.0);
  MockBMMTokenOp filter_op(search_ctx_, 10, 100, 0.0, 0.0);
  ASSERT_EQ(OB_SUCCESS, score_op.init(param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, false, 500));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_TRUE(bmm.filter_after_score_);

  const double thresholds[] = {5.0, 6.0};
  for (int64_t i = 0; i < ARRAYSIZEOF(thresholds); ++i) {
    uint64_t max_id = 0;
    ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.reuse_with_start_id(10, max_id));
    ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(10, 5.0));
    ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(11, 2.0));
    ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(thresholds[i]));
    ASSERT_EQ(OB_SUCCESS, bmm.apply_filter_to_scored_batch());
    EXPECT_TRUE(bmm.doc_score_accumulator_.is_empty());
    EXPECT_DOUBLE_EQ(0.0, bmm.doc_score_accumulator_.get_max_score());
    EXPECT_EQ(0, filter_op.get_advance_to_count());
    EXPECT_TRUE(bmm.curr_filter_id_.is_min());
    EXPECT_FALSE(bmm.filter_end_);
  }
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, candidate_filter_seeks_only_strictly_competitive_docs)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const MockBMMPosting filter_postings[] = {
      {10, 0.0, 100, 0.0}, {11, 0.0, 100, 0.0}, {12, 0.0, 100, 0.0},
      {13, 0.0, 100, 0.0}, {14, 0.0, 100, 0.0}};
  MockBMMTokenOpParam param;
  MockBMMTokenOp score_op(search_ctx_, 10, 100, 7.0, 7.0);
  MockBMMTokenOp filter_op(search_ctx_, filter_postings, ARRAYSIZEOF(filter_postings), 0.0);
  ASSERT_EQ(OB_SUCCESS, score_op.init(param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, false, 500));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5.0));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.reuse_with_start_id(10, max_id));
  const double scores[] = {1.0, 5.0, 6.0, 4.0, 7.0};
  for (int64_t i = 0; i < ARRAYSIZEOF(scores); ++i) {
    ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(10 + i, scores[i]));
  }
  ASSERT_EQ(OB_SUCCESS, bmm.apply_filter_to_scored_batch());
  EXPECT_EQ(2, filter_op.get_advance_to_count());
  EXPECT_EQ(14, bmm.curr_filter_id_.get_uint64());
  DocScoreIter iter;
  uint64_t rowid = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, iter.open(bmm.doc_score_accumulator_));
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(12, rowid);
  EXPECT_DOUBLE_EQ(6.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(14, rowid);
  EXPECT_DOUBLE_EQ(7.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));
  EXPECT_DOUBLE_EQ(7.0, bmm.doc_score_accumulator_.get_max_score());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, candidate_filter_preserves_matches_when_filter_exhausts)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const MockBMMPosting filter_postings[] = {{10, 0.0, 100, 0.0}, {12, 0.0, 100, 0.0}};
  MockBMMTokenOpParam param;
  MockBMMTokenOp score_op(search_ctx_, 10, 100, 9.0, 9.0);
  MockBMMTokenOp filter_op(search_ctx_, filter_postings, ARRAYSIZEOF(filter_postings), 0.0);
  ASSERT_EQ(OB_SUCCESS, score_op.init(param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, false, 500));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.reuse_with_start_id(10, max_id));
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(10, 2.0));
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(12, 3.0));
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(15, 8.0));
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(16, 9.0));
  ASSERT_EQ(OB_SUCCESS, bmm.apply_filter_to_scored_batch());
  EXPECT_TRUE(bmm.filter_end_);
  EXPECT_EQ(3, filter_op.get_advance_to_count());
  EXPECT_DOUBLE_EQ(3.0, bmm.doc_score_accumulator_.get_max_score());
  DocScoreIter iter;
  uint64_t rowid = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, iter.open(bmm.doc_score_accumulator_));
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(10, rowid);
  EXPECT_DOUBLE_EQ(2.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(12, rowid);
  EXPECT_DOUBLE_EQ(3.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, nonessential_batch_pruning_is_not_overwritten)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  MockBMMTokenOpParam param;
  MockBMMTokenOp score_op(search_ctx_, 10, 100, 2.0, 2.0);
  MockBMMTokenOp filter_op(search_ctx_, 10, 100, 0.0, 0.0);
  ASSERT_EQ(OB_SUCCESS, score_op.init(param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, false, 500));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(6.0));

  // Model a pending batch with partial score 3 and remaining upper bound 2.
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.reuse_with_start_id(10, max_id));
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.add_dim(10, 3.0));
  bmm.vec_non_essential_dim_count_ = 1;
  bmm.vec_sorted_iter_idxes_.at(0) = 0;
  bmm.non_ess_block_max_score_suffix_sums_.at(0) = 2.0;
  ASSERT_FALSE(bmm.merge_iters_.at(0)->iter_end());
  ObDASRowID batch_end;
  batch_end.set_uint64(100);
  bool valid = true;
  ASSERT_EQ(OB_SUCCESS, bmm.evaluate_non_essential_batch(batch_end, valid));
  ASSERT_FALSE(valid);
  ASSERT_FALSE(bmm.doc_score_accumulator_.is_empty());

  // With no subsequent essential candidates, the state machine must return EOF.
  ASSERT_TRUE(bmm.row_merger_->is_empty());
  bmm.status_ = ObDASBMMOp::SearchStatus::EVALUATE_VEC_NON_ESSENTIAL;
  EXPECT_EQ(OB_ITER_END, bmm.inner_get_next_batch_scoring_driven());
  EXPECT_EQ(ObDASBMMOp::SearchStatus::FINISHED, bmm.status_);
  EXPECT_EQ(0, filter_op.get_advance_to_count());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, dense_filter_orders_agree_across_windows_advance_and_rescan)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  eval_ctx_.max_batch_size_ = 1;
  const uint64_t ids[] = {1, 2, 65, 66, 129, 130, 193, 194, 257, 258};
  const double scores[] = {2.0, 6.0, 7.0, 8.0, 3.0, 9.0, 4.0, 10.0, 11.0, 11.0};
  const double extra_scores[] = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
  const uint64_t filter_ids[] = {1, 2, 80, 129, 130, 194, 257};
  const double filter_scores[ARRAYSIZEOF(filter_ids)] = {};
  const uint64_t expected_ids[] = {2, 130, 194, 257};
  const double expected_scores[] = {7.0, 10.0, 11.0, 12.0};
  const int64_t filter_costs[] = {1, 500};

  for (int64_t mode = 0; mode < ARRAYSIZEOF(filter_costs); ++mode) {
    SCOPED_TRACE(mode);
    MockBMMTokenOpParam score_param;
    MockBMMTokenOpParam filter_param;
    score_param.set_is_scoring(true);
    MockBMMPostingOp score_op(search_ctx_, ids, scores, ARRAYSIZEOF(ids));
    MockBMMPostingOp extra_op(search_ctx_, ids, extra_scores, ARRAYSIZEOF(ids));
    MockBMMPostingOp filter_op(search_ctx_, filter_ids, filter_scores, ARRAYSIZEOF(filter_ids));
    ASSERT_EQ(OB_SUCCESS, score_op.init(score_param));
    ASSERT_EQ(OB_SUCCESS, extra_op.init(score_param));
    ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
    ObSEArray<ObIDASSearchOp *, 2> score_ops;
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&extra_op));
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
        bmm, score_ops, &filter_op, 1000, 11, false, filter_costs[mode]));
    ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
    ASSERT_EQ(mode == 1, bmm.filter_after_score_);
    ASSERT_EQ(mode == 1 ? 64 : 16, bmm.doc_score_accumulator_.get_capacity());
    ASSERT_EQ(OB_SUCCESS, bmm.open());
    ObDASRowID rowid;
    double score = 0.0;
    for (int64_t pass = 0; pass < 2; ++pass) {
      SCOPED_TRACE(pass);
      if (pass > 0) {
        ASSERT_EQ(OB_SUCCESS, bmm.rescan());
      }
      ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5.0));
      for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
        ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
        EXPECT_EQ(expected_ids[i], rowid.get_uint64());
        EXPECT_DOUBLE_EQ(expected_scores[i], score);
      }
      EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
    }
    ASSERT_EQ(OB_SUCCESS, bmm.rescan());
    ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5.0));
    ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
    EXPECT_EQ(2, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(7.0, score);
    ObDASRowID target;
    target.set_uint64(129);
    ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
    EXPECT_EQ(130, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(10.0, score);
    target.set_uint64(194);
    ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
    EXPECT_EQ(194, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(11.0, score);
    ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
    EXPECT_EQ(257, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(12.0, score);
    EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
    EXPECT_EQ(OB_SUCCESS, bmm.close());
  }
}

TEST_F(TestDASBMM, optional_vectorized_filter_drives_presence_and_scoring)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  eval_ctx_.max_batch_size_ = 1;
  const ScriptedBMMPostingList scoring_postings = ScriptedBMMPostingBuilder()
      .add(2, 2.0, 33, 9.0)
      .add(17, 5.0, 33, 9.0)
      .add(33, 9.0, 33, 9.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 48, 0.0)
      .add(2, 0.0, 48, 0.0)
      .add(16, 0.0, 48, 0.0)
      .add(33, 0.0, 48, 0.0)
      .add(48, 0.0, 48, 0.0)
      .build();
  ScriptedBMMSearchOp scoring_op(search_ctx_, scoring_postings, 9.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  scoring_op.use_batch_drain(false);
  filter_op.use_batch_drain(false);
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, scoring_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));

  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, true));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_TRUE(bmm.query_optional_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(16, bmm.doc_score_accumulator_.get_capacity());
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObSEArray<uint64_t, 8> actual_ids;
  ObSEArray<double, 8> actual_scores;
  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = bmm.next_rowid(rowid, score))) {
    ASSERT_EQ(OB_SUCCESS, actual_ids.push_back(rowid.get_uint64()));
    ASSERT_EQ(OB_SUCCESS, actual_scores.push_back(score));
  }
  EXPECT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(5, actual_ids.count());
  EXPECT_EQ(1, actual_ids.at(0));
  EXPECT_EQ(2, actual_ids.at(1));
  EXPECT_EQ(16, actual_ids.at(2));
  EXPECT_EQ(33, actual_ids.at(3));
  EXPECT_EQ(48, actual_ids.at(4));
  ASSERT_EQ(5, actual_scores.count());
  EXPECT_DOUBLE_EQ(0.0, actual_scores[0]);
  EXPECT_DOUBLE_EQ(2.0, actual_scores[1]);
  EXPECT_DOUBLE_EQ(0.0, actual_scores[2]);
  EXPECT_DOUBLE_EQ(9.0, actual_scores[3]);
  EXPECT_DOUBLE_EQ(0.0, actual_scores[4]);
  // Filter owns the optional result domain. With an expanded vector boundary,
  // the final filter batch may finish before another scoring pull observes EOF.
  EXPECT_TRUE(bmm.filter_end_);
  EXPECT_EQ(ObDASBMMOp::SearchStatus::FINISHED, bmm.status_);

  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.0, score);
  target.set_uint64(1);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(2.0, score);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, optional_vectorized_accumulates_multiple_scoring_dimensions)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  eval_ctx_.max_batch_size_ = 1;
  const ScriptedBMMPostingList first_scoring_postings = ScriptedBMMPostingBuilder()
      .add(2, 2.0, 33, 9.0)
      .add(33, 9.0, 33, 9.0)
      .build();
  const ScriptedBMMPostingList second_scoring_postings = ScriptedBMMPostingBuilder()
      .add(2, 20.0, 34, 100.0)
      .add(16, 40.0, 34, 100.0)
      .add(34, 100.0, 34, 100.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 48, 0.0)
      .add(2, 0.0, 48, 0.0)
      .add(16, 0.0, 48, 0.0)
      .add(33, 0.0, 48, 0.0)
      .add(48, 0.0, 48, 0.0)
      .build();
  ScriptedBMMSearchOp first_scoring_op(search_ctx_, first_scoring_postings, 9.0);
  ScriptedBMMSearchOp second_scoring_op(search_ctx_, second_scoring_postings, 100.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  first_scoring_op.use_batch_drain(false);
  second_scoring_op.use_batch_drain(false);
  filter_op.use_batch_drain(false);
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, first_scoring_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, second_scoring_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));

  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&first_scoring_op));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&second_scoring_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, true));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_TRUE(bmm.query_optional_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(16, bmm.doc_score_accumulator_.get_capacity());
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  const uint64_t expected_ids[] = {1, 2, 16, 33, 48};
  const double expected_scores[] = {0.0, 22.0, 40.0, 9.0, 0.0};
  ObDASRowID rowid;
  double score = 0.0;
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
    ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
    EXPECT_EQ(expected_ids[i], rowid.get_uint64());
    EXPECT_DOUBLE_EQ(expected_scores[i], score);
  }
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, optional_vectorized_threshold_is_strict_and_does_not_replay)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  eval_ctx_.max_batch_size_ = 1;
  const ScriptedBMMPostingList scoring_postings = ScriptedBMMPostingBuilder()
      .add(2, 2.0, 33, 9.0)
      .add(17, 5.0, 33, 9.0)
      .add(33, 9.0, 33, 9.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 1000000, 0.0)
      .add(2, 0.0, 1000000, 0.0)
      .add(16, 0.0, 1000000, 0.0)
      .add(33, 0.0, 1000000, 0.0)
      .add(34, 0.0, 1000000, 0.0)
      .add(1000000, 0.0, 1000000, 0.0)
      .build();
  ScriptedBMMSearchOp scoring_op(search_ctx_, scoring_postings, 9.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  scoring_op.use_batch_drain(false);
  filter_op.use_batch_drain(false);
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, scoring_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));

  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, true));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_TRUE(bmm.query_optional_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(16, bmm.doc_score_accumulator_.get_capacity());
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));
  EXPECT_FALSE(bmm.is_filter_driven_optional());
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(33, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(9.0, score);
  ASSERT_TRUE(bmm.doc_score_accumulator_.is_valid());
  EXPECT_EQ(33, bmm.doc_score_accumulator_.start_id_);
  EXPECT_TRUE(bmm.doc_score_accumulator_.bitset_->exist(0));
  EXPECT_FALSE(bmm.doc_score_accumulator_.bitset_->exist(1));
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  EXPECT_FALSE(bmm.filter_end_);
  EXPECT_EQ(ObDASBMMOp::SearchStatus::FINISHED, bmm.status_);

  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  EXPECT_TRUE(bmm.is_filter_driven_optional());
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));
  target.set_uint64(32);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(33, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(9.0, score);
  EXPECT_FALSE(bmm.is_filter_driven_optional());
  EXPECT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  EXPECT_FALSE(bmm.filter_end_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, optional_vectorized_advance_after_scoring_eof)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  eval_ctx_.max_batch_size_ = 1;
  const ScriptedBMMPostingList scoring_postings = ScriptedBMMPostingBuilder()
      .add(2, 2.0, 2, 2.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 33, 0.0)
      .add(2, 0.0, 33, 0.0)
      .add(17, 0.0, 33, 0.0)
      .add(33, 0.0, 33, 0.0)
      .build();
  ScriptedBMMSearchOp scoring_op(search_ctx_, scoring_postings, 2.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  scoring_op.use_batch_drain(false);
  filter_op.use_batch_drain(false);
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, scoring_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));

  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, true));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_TRUE(bmm.query_optional_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(16, bmm.doc_score_accumulator_.get_capacity());
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID target;
  ObDASRowID rowid;
  double score = -1.0;
  target.set_uint64(16);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(17, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.0, score);
  EXPECT_TRUE(bmm.is_filter_driven_optional());
  EXPECT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(33, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.0, score);
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, optional_vectorized_advance_to_uint64_tail)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  eval_ctx_.max_batch_size_ = 1;
  const ScriptedBMMPostingList scoring_postings = ScriptedBMMPostingBuilder()
      .add(2, 2.0, 2, 2.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, UINT64_MAX - 1, 0.0)
      .add(UINT64_MAX - 1, 0.0, UINT64_MAX - 1, 0.0)
      .build();
  ScriptedBMMSearchOp scoring_op(search_ctx_, scoring_postings, 2.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  scoring_op.use_batch_drain(false);
  filter_op.use_batch_drain(false);
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, scoring_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));

  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring_op));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, &filter_op, 1000, 11, true));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_TRUE(bmm.query_optional_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(16, bmm.doc_score_accumulator_.get_capacity());
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID target;
  ObDASRowID rowid;
  double score = -1.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  target.set_uint64(UINT64_MAX - 1);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(UINT64_MAX - 1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.0, score);
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));

  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  target.set_max();
  EXPECT_EQ(OB_ITER_END, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Derive exact window bounds from postings instead of reusing a constant in later windows.
class ExactRangeBMMTokenOp : public MockBMMTokenOp
{
public:
  ExactRangeBMMTokenOp(ObDASSearchCtx &ctx, const MockBMMPosting *postings,
      const int64_t count, const double global_max)
    : MockBMMTokenOp(ctx, postings, count, global_max), postings_(postings), count_(count)
  { enable_advance_to_id(); }
private:
  int do_calc_range_max_score(const ObDASRowID &start, const bool inclusive,
      const ObDASRowID &end, double &max_score, bool &can_calc) override
  {
    max_score = 0.0;
    can_calc = true;
    for (int64_t i = 0; i < count_; ++i) {
      const MockBMMPosting &p = postings_[i];
      if ((p.doc_id_ > start.get_uint64() || (inclusive && p.doc_id_ == start.get_uint64()))
          && (end.is_max() || p.doc_id_ <= end.get_uint64())) {
        max_score = std::max(max_score, p.score_);
      }
    }
    return OB_SUCCESS;
  }
  const MockBMMPosting *postings_;
  int64_t count_;
};

TEST_F(TestDASBMM, range_max_boundary_gap_restores_only_needed_dimensions)
{
  for (int variant = 0; variant < 3; ++variant) {
    SCOPED_TRACE(::testing::Message() << "variant=" << variant);
    MockBMMTokenOpParam param;
    const MockBMMPosting a[] = {{1, 10.0, 16384, 10.0}, {40000, 10.0, 50000, 10.0}};
    const MockBMMPosting b[] = {{20000, 20.0, 30000, 20.0}};
    const MockBMMPosting c[] = {{50000, 1.0, 50000, 1.0}};
    ExactRangeBMMTokenOp token_a(search_ctx_, a, ARRAYSIZEOF(a), 10.0);
    ExactRangeBMMTokenOp token_b(search_ctx_, b, ARRAYSIZEOF(b), 20.0);
    ExactRangeBMMTokenOp token_c(search_ctx_, c, ARRAYSIZEOF(c), 1.0);
    ASSERT_EQ(OB_SUCCESS, token_a.init(param));
    ASSERT_EQ(OB_SUCCESS, token_b.init(param));
    ASSERT_EQ(OB_SUCCESS, token_c.init(param));
    ObSEArray<ObIDASSearchOp *, 3> ops;
    ASSERT_EQ(OB_SUCCESS, ops.push_back(&token_a));
    ASSERT_EQ(OB_SUCCESS, ops.push_back(&token_b));
    ASSERT_EQ(OB_SUCCESS, ops.push_back(&token_c));
    ObDASBMMOp bmm(search_ctx_);
    ObDASBMMOpParam bmm_param(ops, nullptr, allocator_, ObDASBMMBuildOptions());
    ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
    ASSERT_EQ(OB_SUCCESS, prepare_vec_runtime_arrays(bmm, ops.count()));
    ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
    bmm.min_competitive_score_ = variant == 1 ? 0.5 : 5.0;
    ObDASRowID target;
    target.set_uint64(1);
    ASSERT_EQ(OB_SUCCESS, bmm.compute_vec_boundary_at(target));
    ASSERT_EQ(2, bmm.vec_non_essential_dim_count_);
    ASSERT_EQ(16384, bmm.vec_boundary_id_.get_uint64());
    DocScoreAccumulator acc;
    ASSERT_EQ(OB_SUCCESS, acc.init(16384, allocator_));
    uint64_t end = 0;
    ASSERT_EQ(OB_SUCCESS, acc.reuse_with_start_id(1, end));
    target.set_uint64(16384);
    ASSERT_EQ(OB_SUCCESS, bmm.row_merger_->merge_one_batch(target, acc));
    ObDASRowID top;
    ASSERT_EQ(OB_SUCCESS, bmm.row_merger_->get_top_id(top));
    ASSERT_EQ(40000, top.get_uint64());
    const int64_t a_seek = token_a.get_advance_to_count();
    if (variant == 2) {
      // A rising threshold makes the cached global-coverage fast path sufficient.
      bmm.min_competitive_score_ = 21.0;
      EXPECT_EQ(OB_ITER_END, bmm.compute_vec_boundary());
    } else {
      ASSERT_EQ(OB_SUCCESS, bmm.compute_vec_boundary());
      ASSERT_EQ(OB_SUCCESS, bmm.row_merger_->get_top_id(top));
      EXPECT_EQ(20000, top.get_uint64());
    }
    EXPECT_EQ(variant == 2 ? 0 : 1, token_b.get_advance_to_id_count());
    EXPECT_EQ(0, token_a.get_advance_to_id_count());
    EXPECT_EQ(variant == 1 ? 1 : 0, token_c.get_advance_to_id_count());
    EXPECT_EQ(a_seek, token_a.get_advance_to_count());
    EXPECT_EQ(0, token_c.get_advance_to_count());
    EXPECT_EQ(OB_SUCCESS, bmm.close());
  }
}

TEST_F(TestDASBMM, range_max_boundary_gap_public_iteration)
{
  const uint64_t bases[] = {0, UINT64_MAX - 50000};
  for (const uint64_t base : bases) {
    for (int mode = 0; mode < 4; ++mode) {
      for (int essential_eof = 0; essential_eof < 2; ++essential_eof) {
        SCOPED_TRACE(::testing::Message() << "base=" << base << " mode=" << mode << " eof=" << essential_eof);
        BMMVectorizedMergeConfigGuard guard;
        MockBMMTokenOpParam param;
        param.set_is_scoring(true);
        MockBMMTokenOpParam filter_param;
        const MockBMMPosting a[] = {{base + 1, 10.0, base + 16384, 10.0}, {base + 40000, 10.0, base + 50000, 10.0}};
        // Non-EOF variants also require finishing the old buffer's non-essential score.
        const MockBMMPosting b[] = {
          {base + 1, 2.0, base + 16384, 2.0}, {base + 20000, 20.0, base + 30000, 20.0}};
        const MockBMMPosting c[] = {{base + 50000, 1.0, base + 50000, 1.0}};
        const MockBMMPosting f[] = {
          {base + 1, 0.0, base + 50000, 0.0}, {base + 20000, 0.0, base + 50000, 0.0}, {base + 40000, 0.0, base + 50000, 0.0}};
        const uint64_t filter_ids[] = {base + 1, base + 20000, base + 40000};
        ExactRangeBMMTokenOp token_a(search_ctx_, a, essential_eof ? 1 : 2, 10.0);
        ExactRangeBMMTokenOp token_b(search_ctx_, b + essential_eof, 2 - essential_eof, 20.0);
        ExactRangeBMMTokenOp token_c(search_ctx_, c, ARRAYSIZEOF(c), 1.0);
        MockBMMTokenOp filter(search_ctx_, f, ARRAYSIZEOF(f), 0.0);
        MockDirectBMMFilterOp direct_filter(search_ctx_, filter_ids, ARRAYSIZEOF(filter_ids));
        ASSERT_EQ(OB_SUCCESS, token_a.init(param));
        ASSERT_EQ(OB_SUCCESS, token_b.init(param));
        ASSERT_EQ(OB_SUCCESS, token_c.init(param));
        ASSERT_EQ(OB_SUCCESS, filter.init(filter_param));
        ASSERT_EQ(OB_SUCCESS, direct_filter.init(filter_param));
        ObSEArray<ObIDASSearchOp *, 3> ops;
        ASSERT_EQ(OB_SUCCESS, ops.push_back(&token_a));
        ASSERT_EQ(OB_SUCCESS, ops.push_back(&token_b));
        ASSERT_EQ(OB_SUCCESS, ops.push_back(&token_c));
        ObDASBMMOp bmm(search_ctx_);
        if (3 == mode) {
          ObDASBMMOpParam bmm_param(
              ops, &direct_filter, allocator_,
              ObDASBMMBuildOptions(true, ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN));
          bmm_param.set_is_scoring(true);
          ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
        } else {
          ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
              bmm, ops, mode == 0 ? nullptr : &filter, 100000, mode == 2 ? 10 : 10000));
        }
        ASSERT_TRUE(bmm.enable_vec_merge_);
        ASSERT_EQ(mode == 2 ? ObDASBMMOp::FilterMergeMode::SPARSE_SEEK
            : ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
        ASSERT_EQ(OB_SUCCESS, bmm.open());
        ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5.0));
        ObDASRowID rowid;
        double score = 0.0;
        ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
        EXPECT_EQ(base + 1, rowid.get_uint64());
        EXPECT_DOUBLE_EQ(essential_eof ? 10.0 : 12.0, score);
        ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
        EXPECT_EQ(base + 20000, rowid.get_uint64());
        EXPECT_DOUBLE_EQ(20.0, score);
        if (!essential_eof) {
          ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
          EXPECT_EQ(base + 40000, rowid.get_uint64());
          EXPECT_DOUBLE_EQ(10.0, score);
        }
        EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
        EXPECT_EQ(OB_SUCCESS, bmm.close());
      }
    }
  }
}

TEST_F(TestDASBMM, optional_threshold_switch_resumes_unfinished_window)
{
  for (int sparse = 0; sparse < 2; ++sparse) {
    SCOPED_TRACE(::testing::Message() << "sparse=" << sparse);
    BMMVectorizedMergeConfigGuard guard;
    eval_ctx_.max_batch_size_ = 1; // A 16-ID batch ends before the next scoring row.
    MockBMMTokenOpParam score_param;
    MockBMMTokenOpParam filter_param;
    score_param.set_is_scoring(true);
    const MockBMMPosting scoring[] = {{2, 2.0, 100, 9.0}, {25, 9.0, 100, 9.0}};
    const MockBMMPosting filters[] = {
      {1, 0.0, 1000000, 0.0}, {2, 0.0, 1000000, 0.0},
      {25, 0.0, 1000000, 0.0}, {26, 0.0, 1000000, 0.0}, {1000000, 0.0, 1000000, 0.0}};
    MockBMMTokenOp token(search_ctx_, scoring, ARRAYSIZEOF(scoring), 9.0);
    MockBMMTokenOp filter(search_ctx_, filters, ARRAYSIZEOF(filters), 0.0);
    ASSERT_EQ(OB_SUCCESS, token.init(score_param));
    ASSERT_EQ(OB_SUCCESS, filter.init(filter_param));
    ObSEArray<ObIDASSearchOp *, 1> ops;
    ASSERT_EQ(OB_SUCCESS, ops.push_back(&token));
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
        bmm, ops, &filter, 100000, sparse ? 10 : 10000, true));
    ASSERT_TRUE(bmm.enable_vec_merge_);
    ASSERT_EQ(OB_SUCCESS, bmm.open());
    ObDASRowID rowid;
    double score = 0.0;
    ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
    EXPECT_EQ(1, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(0.0, score);
    ASSERT_GT(bmm.vec_boundary_id_.get_uint64(), 25);
    ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(2.0));
    ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
    EXPECT_EQ(25, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(9.0, score);
    EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
    EXPECT_EQ(OB_SUCCESS, bmm.close());
  }
}

TEST_F(TestDASBMM, range_max_refines_vectorized_partition)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  ScriptedBMMSearchOpParam token_param;
  token_param.set_is_scoring(true);
  // Local bounds dominate actual rows; global bounds remain deliberately loose.
  const ScriptedBMMPostingList postings_a = {{11, 0.5, 1000, 4.0}};
  const ScriptedBMMPostingList postings_b = {{11, 1.0, 1000, 5.0}};
  const ScriptedBMMPostingList postings_c = {{11, 1.5, 1000, 6.0}};
  const ScriptedBMMPostingList postings_d = {{11, 9.0, 1000, 9.0}};
  ScriptedBMMSearchOp token_a(search_ctx_, postings_a, 4.0);
  ScriptedBMMSearchOp token_b(search_ctx_, postings_b, 5.0);
  ScriptedBMMSearchOp token_c(search_ctx_, postings_c, 6.0);
  ScriptedBMMSearchOp token_d(search_ctx_, postings_d, 9.0);
  token_a.set_range_max_score(0.5);
  token_b.set_range_max_score(1.0);
  token_c.set_range_max_score(1.5);
  token_d.set_range_max_score(9.0);
  ASSERT_EQ(OB_SUCCESS, token_a.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_b.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_c.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_d.init(token_param));

  ObSEArray<ObIDASSearchOp *, 4> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_b));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_c));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_d));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, nullptr, 100000, 1000));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(10.0));
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(11, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(12.0, score);

  // Global maxima retain two non-essential dimensions, while the frozen-window
  // maxima safely move the third dimension out of the essential merger.
  EXPECT_EQ(2, bmm.non_essential_dim_count_);
  EXPECT_EQ(3, bmm.vec_non_essential_dim_count_);
  EXPECT_DOUBLE_EQ(3.0, bmm.vec_non_essential_dim_max_score_);
  EXPECT_DOUBLE_EQ(3.0, bmm.non_ess_block_max_score_suffix_sums_.at(0));
  EXPECT_DOUBLE_EQ(2.5, bmm.non_ess_block_max_score_suffix_sums_.at(1));
  EXPECT_DOUBLE_EQ(1.5, bmm.non_ess_block_max_score_suffix_sums_.at(2));
  EXPECT_EQ(11 + 4096 * 4 - 1, bmm.vec_boundary_id_.get_uint64());
  EXPECT_EQ(1, token_a.get_calc_range_max_score_count());
  EXPECT_EQ(1, token_b.get_calc_range_max_score_count());
  EXPECT_EQ(1, token_c.get_calc_range_max_score_count());
  EXPECT_EQ(1, token_d.get_calc_range_max_score_count());

  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, small_disjunction_skips_range_max_collection)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  ScriptedBMMSearchOpParam token_param;
  ScriptedBMMSearchOp token_a(search_ctx_, 5, 20, 4.0, 4.0);
  ScriptedBMMSearchOp token_b(search_ctx_, 7, 100, 8.0, 8.0);
  token_a.set_range_max_score(0.5);
  token_b.set_range_max_score(1.0);
  ASSERT_EQ(OB_SUCCESS, token_a.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_b.init(token_param));

  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_b));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(bmm, score_ops, nullptr, 100000, 1000));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW, bmm.filter_merge_mode_);
  ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
  bmm.min_competitive_score_ = 5.0;

  ObDASRowID target;
  target.set_uint64(1);
  ASSERT_EQ(OB_SUCCESS, bmm.compute_vec_boundary_at(target));
  EXPECT_EQ(0, token_a.get_calc_range_max_score_count());
  EXPECT_EQ(0, token_b.get_calc_range_max_score_count());
  EXPECT_EQ(bmm.non_essential_dim_count_, bmm.vec_non_essential_dim_count_);
  EXPECT_EQ(4096 * 4, bmm.vec_boundary_id_.get_uint64());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, direct_filter_bitset_accepts_unordered_callbacks_and_duplicates)
{
  MockBMMTokenOpParam filter_param;
  MockCallbackUnorderedDirectBMMFilterOp filter(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, filter.init(filter_param));
  ASSERT_EQ(OB_SUCCESS, filter.open());

  ObDASBMMOp bmm(search_ctx_);
  const int64_t capacity = eval_ctx_.max_batch_size_ * 4;
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.init(capacity, allocator_));
  void *filter_buf = allocator_.alloc(ObBitVector::memory_size(capacity));
  ASSERT_NE(nullptr, filter_buf);
  bmm.filter_bitset_ = to_bit_vector(filter_buf);
  bmm.filter_bitset_->init(capacity);
  bmm.filter_op_ = &filter;

  ObDASRowID batch_start_id;
  ObDASRowID batch_end_id;
  batch_start_id.set_uint64(100);
  batch_end_id.set_uint64(110);
  bool has_hit = false;
  ASSERT_EQ(OB_SUCCESS, bmm.build_direct_filter_bitset(
      batch_start_id, batch_end_id, has_hit));
  EXPECT_TRUE(has_hit);
  EXPECT_TRUE(bmm.filter_bitset_->exist(1));
  EXPECT_TRUE(bmm.filter_bitset_->exist(3));
  EXPECT_TRUE(bmm.filter_bitset_->exist(5));
  EXPECT_FALSE(bmm.filter_bitset_->exist(0));
  EXPECT_FALSE(bmm.filter_bitset_->exist(2));
  EXPECT_FALSE(bmm.filter_bitset_->exist(4));
  EXPECT_FALSE(bmm.filter_bitset_->exist(6));
  EXPECT_EQ(OB_SUCCESS, filter.close());
}

TEST_F(TestDASBMM, direct_filter_reuses_lookahead_across_windows)
{
  static constexpr uint64_t FILTER_ID = 250;
  MockBMMTokenOpParam filter_param;
  MockDirectBMMFilterOp filter(search_ctx_, FILTER_ID);
  ASSERT_EQ(OB_SUCCESS, filter.init(filter_param));
  ASSERT_EQ(OB_SUCCESS, filter.open());

  ObDASBMMOp bmm(search_ctx_);
  const int64_t capacity = eval_ctx_.max_batch_size_ * 4;
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.init(capacity, allocator_));
  void *filter_buf = allocator_.alloc(ObBitVector::memory_size(capacity));
  ASSERT_NE(nullptr, filter_buf);
  bmm.filter_bitset_ = to_bit_vector(filter_buf);
  bmm.filter_bitset_->init(capacity);
  bmm.filter_op_ = &filter;
  bmm.filter_mode_ = ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN;

  ObDASRowID batch_start_id;
  ObDASRowID batch_end_id;
  uint64_t max_batch_id = 0;
  bool need_evaluate_batch = true;
  batch_start_id.set_uint64(100);
  batch_end_id.set_uint64(199);
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.reuse_with_start_id(
      batch_start_id.get_uint64(), max_batch_id));
  ASSERT_EQ(OB_SUCCESS, bmm.prepare_filter_for_vectorized_batch(
      batch_start_id, batch_end_id, need_evaluate_batch));
  EXPECT_FALSE(need_evaluate_batch);
  EXPECT_EQ(FILTER_ID, bmm.curr_filter_id_.get_uint64());
  EXPECT_EQ(1, filter.get_drain_count());

  batch_start_id.set_uint64(200);
  batch_end_id.set_uint64(300);
  need_evaluate_batch = true;
  ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.reuse_with_start_id(
      batch_start_id.get_uint64(), max_batch_id));
  ASSERT_EQ(OB_SUCCESS, bmm.prepare_filter_for_vectorized_batch(
      batch_start_id, batch_end_id, need_evaluate_batch));
  EXPECT_TRUE(need_evaluate_batch);
  EXPECT_TRUE(bmm.filter_bitset_->exist(
      FILTER_ID - batch_start_id.get_uint64()));
  EXPECT_TRUE(bmm.filter_end_);
  EXPECT_TRUE(bmm.curr_filter_id_.is_max());
  EXPECT_EQ(2, filter.get_drain_count());

  batch_start_id.set_uint64(301);
  batch_end_id.set_uint64(400);
  need_evaluate_batch = true;
  ASSERT_EQ(OB_SUCCESS, bmm.prepare_filter_for_vectorized_batch(
      batch_start_id, batch_end_id, need_evaluate_batch));
  EXPECT_FALSE(need_evaluate_batch);
  EXPECT_EQ(2, filter.get_drain_count());
  EXPECT_EQ(OB_SUCCESS, filter.close());
}

TEST_F(TestDASBMM, dense_filter_boundary_crossing_preserves_rows)
{
  const uint64_t boundary_id = 16384;
  const uint64_t next_ids[] = {boundary_id + 1, 20000, 1000000000000ULL};
  // Compare direct drain with ordered, unfiltered and scalar execution.
  for (int64_t filter_mode = 0; filter_mode < 4; ++filter_mode) {
    for (const uint64_t next_id : next_ids) {
      SCOPED_TRACE(::testing::Message() << "filter_mode=" << filter_mode
          << " next_id=" << next_id);
      const bool vectorized = 3 != filter_mode;
      const MockBMMPosting first_postings[] = {
        {1, 1.0, boundary_id, 4.0},
        {boundary_id, 4.0, boundary_id, 4.0},
        {next_id, 6.0, next_id, 6.0},
      };
      const MockBMMPosting second_postings[] = {
        {1, 2.0, boundary_id, 3.0},
        {boundary_id, 3.0, boundary_id, 3.0},
        {next_id, 4.0, next_id, 4.0},
      };
      const uint64_t filter_ids[] = {1, boundary_id, next_id};
      const double filter_scores[] = {0.0, 0.0, 0.0};
      const double expected_scores[] = {3.0, 7.0, 10.0};
      MockBMMTokenOpParam score_param;
      MockBMMTokenOpParam filter_param;
      score_param.set_is_scoring(true);
      MockBMMTokenOp first_token(search_ctx_, first_postings, 3, 6.0);
      MockBMMTokenOp second_token(search_ctx_, second_postings, 3, 4.0);
      MockBMMPostingOp ordered_filter(search_ctx_, filter_ids, filter_scores, 3);
      MockDirectBMMFilterOp direct_filter(search_ctx_, filter_ids, 3);
      ASSERT_EQ(OB_SUCCESS, first_token.init(score_param));
      ASSERT_EQ(OB_SUCCESS, second_token.init(score_param));
      ASSERT_EQ(OB_SUCCESS, ordered_filter.init(filter_param));
      ASSERT_EQ(OB_SUCCESS, direct_filter.init(filter_param));
      ObSEArray<ObIDASSearchOp *, 2> score_ops;
      ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&first_token));
      ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&second_token));
      ObIDASSearchOp *filter = 0 == filter_mode ? nullptr
          : (2 == filter_mode ? static_cast<ObIDASSearchOp *>(&direct_filter) : &ordered_filter);
      ObDASBMMOpParam param(
          score_ops, filter, allocator_, ObDASBMMBuildOptions(vectorized,
              2 == filter_mode ? ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN
                  : ObDASBMMBuildOptions::FilterMode::ORDERED));
      param.set_is_scoring(true);
      ObDASBMMOp bmm(search_ctx_);
      ASSERT_EQ(OB_SUCCESS, bmm.init(param));
      ASSERT_EQ(vectorized, bmm.enable_vec_merge_);
      ASSERT_EQ(OB_SUCCESS, bmm.open());
      for (int64_t scan = 0; scan < 2; ++scan) {
        SCOPED_TRACE(::testing::Message() << "scan=" << scan);
        if (scan > 0) {
          ASSERT_EQ(OB_SUCCESS, bmm.rescan());
        }
        // Two scoring dimensions and the initial threshold exclude Range Max pruning.
        ASSERT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, bmm.min_competitive_score_);
        ObDASRowID rowid;
        double score = 0.0;
        for (int64_t i = 0; i < 3; ++i) {
          ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
          EXPECT_EQ(filter_ids[i], rowid.get_uint64());
          EXPECT_DOUBLE_EQ(expected_scores[i], score);
          if (vectorized && i < 2) {
            // A hit exactly on the boundary still belongs to the current window.
            EXPECT_EQ(boundary_id, bmm.vec_boundary_id_.get_uint64());
          }
        }
        EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
        EXPECT_EQ(0, first_token.get_calc_range_max_score_count());
        EXPECT_EQ(0, second_token.get_calc_range_max_score_count());
        if (2 == filter_mode) {
          EXPECT_EQ(3, direct_filter.get_drain_count());
        }
      }
      ASSERT_EQ(OB_SUCCESS, bmm.close());
    }
  }
}

TEST_F(TestDASBMM, direct_filter_handoff_skips_unscored_filter_rows)
{
  const MockBMMPosting postings[] = {
    {1, 1.0, 16384, 1.0}, {40000, 1.0, 56383, 1.0},
  };
  const uint64_t filter_ids[] = {1, 5000, 40000};
  const double filter_scores[] = {0.0, 0.0, 0.0};
  for (const bool direct : {false, true}) {
    SCOPED_TRACE(::testing::Message() << "direct=" << direct);
    MockBMMTokenOpParam score_param;
    MockBMMTokenOpParam filter_param;
    score_param.set_is_scoring(true);
    MockBMMTokenOp first_token(search_ctx_, postings, ARRAYSIZEOF(postings), 1.0);
    MockBMMTokenOp second_token(search_ctx_, postings, ARRAYSIZEOF(postings), 1.0);
    MockBMMPostingOp ordered_filter(search_ctx_, filter_ids, filter_scores, ARRAYSIZEOF(filter_ids));
    MockEnumerableDirectBMMFilterOp direct_filter(search_ctx_, filter_ids, ARRAYSIZEOF(filter_ids));
    ASSERT_EQ(OB_SUCCESS, first_token.init(score_param));
    ASSERT_EQ(OB_SUCCESS, second_token.init(score_param));
    ASSERT_EQ(OB_SUCCESS, ordered_filter.init(filter_param));
    ASSERT_EQ(OB_SUCCESS, direct_filter.init(filter_param));
    ObSEArray<ObIDASSearchOp *, 2> score_ops;
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&first_token));
    ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&second_token));
    ObIDASSearchOp *filter = direct
        ? static_cast<ObIDASSearchOp *>(&direct_filter) : &ordered_filter;
    ObDASBMMOpParam bmm_param(
        score_ops, filter, allocator_, ObDASBMMBuildOptions(true,
            direct ? ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN
                : ObDASBMMBuildOptions::FilterMode::ORDERED));
    bmm_param.set_is_scoring(true);
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
    ASSERT_EQ(OB_SUCCESS, bmm.open());
    for (int64_t scan = 0; scan < 2; ++scan) {
      SCOPED_TRACE(::testing::Message() << "scan=" << scan);
      if (scan > 0) {
        ASSERT_EQ(OB_SUCCESS, bmm.rescan());
      }
      ObDASRowID rowid;
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
      EXPECT_EQ(1, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(2.0, score);
      // The first batch leaves an unscored filter hit inside the old boundary.
      // Direct drain must not be advanced through the ordered iterator API.
      ASSERT_EQ(5000, bmm.curr_filter_id_.get_uint64());
      ASSERT_EQ(16384, bmm.vec_boundary_id_.get_uint64());
      ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
      EXPECT_EQ(40000, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(2.0, score);
      EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
      if (direct) {
        EXPECT_EQ(2, direct_filter.get_drain_count());
      }
    }
    EXPECT_EQ(OB_SUCCESS, bmm.close());
  }
}

TEST_F(TestDASBMM, reuse_dim_max_score_partition_when_state_unchanged)
{
  ScriptedBMMSearchOpParam token_param;
  // 使用与扩窗用例相同的全局上界，首次分组结果为 A | B、C。
  ScriptedBMMSearchOp token_a(search_ctx_, 5, 20, 2.0, 6.0);
  ScriptedBMMSearchOp token_b(search_ctx_, 7, 100, 3.0, 7.0);
  ScriptedBMMSearchOp token_c(search_ctx_, 9, 1000, 8.0, 8.0);
  ASSERT_EQ(OB_SUCCESS, token_a.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_b.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token_c.init(token_param));

  // 将 mock token 安装到真实 BMM 算子，并建立全局 max-score 排序。
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_b));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_c));
  ObDASBMMOp bmm(search_ctx_);
  ObDASBMMOpParam bmm_param(score_ops, nullptr, allocator_, ObDASBMMBuildOptions());
  ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
  ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
  bmm.min_competitive_score_ = 10.0;

  // 清除 merge iter 的全局上界缓存，使首次分组读取 dim_max_score 的行为可以被 mock 计数。
  for (int64_t i = 0; i < bmm.merge_iters_.count(); ++i) {
    bmm.merge_iters_.at(i)->max_score_calculated_ = false;
  }
  const int64_t token_a_count_before_partition = token_a.get_calc_max_score_count();
  const int64_t token_b_count_before_partition = token_b.get_calc_max_score_count();
  ObDASRowID first_target;
  first_target.set_uint64(1);
  bool use_all_dims = false;
  ASSERT_EQ(OB_SUCCESS, bmm.repartition_vec_dims_at(first_target, true, use_all_dims));
  EXPECT_FALSE(use_all_dims);
  EXPECT_EQ(1, bmm.non_essential_dim_count_);
  EXPECT_GT(token_a.get_calc_max_score_count(), token_a_count_before_partition);
  EXPECT_GT(token_b.get_calc_max_score_count(), token_b_count_before_partition);

  // 再次清除 merge iter 缓存，使任何冗余 get_max_score 调用都会增加 mock 计数。
  for (int64_t i = 0; i < bmm.merge_iters_.count(); ++i) {
    bmm.merge_iters_.at(i)->max_score_calculated_ = false;
  }
  const int64_t token_a_count_after_partition = token_a.get_calc_max_score_count();
  const int64_t token_b_count_after_partition = token_b.get_calc_max_score_count();
  const int64_t token_c_count_after_partition = token_c.get_calc_max_score_count();

  // 阈值和结束 token 数量均未变化，第二次 boundary 刷新应直接复用首次分组。
  ObDASRowID second_target;
  second_target.set_uint64(2);
  ASSERT_EQ(OB_SUCCESS, bmm.repartition_vec_dims_at(second_target, true, use_all_dims));
  EXPECT_FALSE(use_all_dims);
  EXPECT_EQ(1, bmm.non_essential_dim_count_);
  EXPECT_EQ(token_a_count_after_partition, token_a.get_calc_max_score_count());
  EXPECT_EQ(token_b_count_after_partition, token_b.get_calc_max_score_count());
  EXPECT_EQ(token_c_count_after_partition, token_c.get_calc_max_score_count());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, provider_word_windows_preserve_required_results)
{
  verify_provider_word_windows(false);
}

TEST_F(TestDASBMM, provider_word_windows_keep_optional_zero_score_fallback)
{
  verify_provider_word_windows(true);
}

TEST_F(TestDASBMM, query_optional_switches_to_sparse_after_dense_window)
{
  BMMVectorizedMergeConfigGuard config_guard;
  const ScriptedBMMPostingList scoring_postings = ScriptedBMMPostingBuilder()
      .add(1, 1.0, 21, 8.0)
      .add(2, 5.0, 21, 8.0)
      .add(17, 6.0, 21, 8.0)
      .add(18, 7.0, 21, 8.0)
      .add(21, 8.0, 21, 8.0)
      .build();
  const ScriptedBMMPostingList filter_postings = ScriptedBMMPostingBuilder()
      .add(1, 0.0, 21, 0.0)
      .add(2, 0.0, 21, 0.0)
      .add(3, 0.0, 21, 0.0)
      .add(17, 0.0, 21, 0.0)
      .add(18, 0.0, 21, 0.0)
      .add(21, 0.0, 21, 0.0)
      .build();
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  ScriptedBMMSearchOp scoring_op(search_ctx_, scoring_postings, 8.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, filter_postings, 0.0);
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  const bool query_optional = true;
  ObDASBMMOp optional_bmm(search_ctx_);
  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;

  ASSERT_TRUE(config_guard.is_valid());
  eval_ctx_.max_batch_size_ = 1;
  scoring_op.use_batch_drain(false);
  filter_op.use_batch_drain(false);
  score_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, scoring_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring_op));
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      optional_bmm, score_ops, &filter_op, 1000, 1, query_optional));
  ASSERT_TRUE(optional_bmm.enable_vec_merge_);
  ASSERT_TRUE(optional_bmm.query_optional_);
  ASSERT_EQ(&filter_op, optional_bmm.filter_op_);
  ASSERT_EQ(ObDASBMMOp::FilterMergeMode::SPARSE_SEEK,
      optional_bmm.filter_merge_mode_);
  ASSERT_TRUE(optional_bmm.is_filter_driven_optional());
  // Query-optional filter-before-score keeps 16 SQL batches per scoring window.
  ASSERT_EQ(16, optional_bmm.doc_score_accumulator_.get_capacity());
  ASSERT_NE(nullptr, optional_bmm.filter_bitset_);
  ASSERT_EQ(ObDAATMergeIter::DRAIN_BATCH_SIZE,
      optional_bmm.sparse_candidate_buffer_.get_capacity());
  ASSERT_EQ(OB_SUCCESS, optional_bmm.open());

  // The first window is filter-driven. Raising the threshold keeps the remaining
  // materialized rows in this window and applies the latest strict threshold to them.
  ASSERT_EQ(OB_SUCCESS, optional_bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(1.0, score);
  ASSERT_EQ(OB_SUCCESS, optional_bmm.set_min_competitive_score(4.0));
  EXPECT_FALSE(optional_bmm.is_filter_driven_optional());
  ASSERT_EQ(OB_SUCCESS, optional_bmm.next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(5.0, score);

  // The next refill is the safe switch point. Sparse seek now returns only
  // filter-and-scoring intersections whose score strictly beats the threshold.
  ASSERT_EQ(OB_SUCCESS, optional_bmm.next_rowid(rowid, score));
  EXPECT_EQ(17, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(6.0, score);
  ASSERT_EQ(OB_SUCCESS, optional_bmm.next_rowid(rowid, score));
  EXPECT_EQ(18, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(7.0, score);
  ASSERT_EQ(OB_SUCCESS, optional_bmm.next_rowid(rowid, score));
  EXPECT_EQ(21, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(8.0, score);
  EXPECT_EQ(OB_ITER_END, optional_bmm.next_rowid(rowid, score));

  // Rescan restores the initial filter-driven mode. An advance beyond the
  // current dense window switches only after that window has been discarded safely.
  ASSERT_EQ(OB_SUCCESS, optional_bmm.rescan());
  EXPECT_EQ(ObDASBMMOp::FilterMergeMode::SPARSE_SEEK,
      optional_bmm.filter_merge_mode_);
  EXPECT_TRUE(optional_bmm.is_filter_driven_optional());
  ASSERT_EQ(OB_SUCCESS, optional_bmm.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, optional_bmm.set_min_competitive_score(4.0));
  target.set_uint64(17);
  ASSERT_EQ(OB_SUCCESS, optional_bmm.advance_to(target, rowid, score));
  EXPECT_EQ(17, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(6.0, score);
  EXPECT_EQ(OB_SUCCESS, optional_bmm.close());
}

TEST_F(TestDASBMM, query_optional_conjunction_filter_disables_vectorized_merge)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam first_filter_param;
  ScriptedBMMSearchOpParam second_filter_param;
  ScriptedBMMSearchOp score_op(search_ctx_, 5, 100, 5.0, 5.0);
  ScriptedBMMSearchOp first_filter_op(search_ctx_, 5, 100, 0.0, 0.0);
  ScriptedBMMSearchOp second_filter_op(search_ctx_, 5, 100, 0.0, 0.0);
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ObSEArray<ObIDASSearchOp *, 2> filter_ops;
  ObDASConjunctionOpParam conjunction_param(filter_ops);
  const bool query_optional = true;
  ObDASConjunctionOp conjunction_op(search_ctx_);
  ObDASBMMOp optional_bmm(search_ctx_);

  ASSERT_TRUE(config_guard.is_valid());
  score_param.set_is_scoring(true);
  conjunction_param.set_is_scoring(false);
  ASSERT_EQ(OB_SUCCESS, score_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, first_filter_op.init(first_filter_param));
  ASSERT_EQ(OB_SUCCESS, second_filter_op.init(second_filter_param));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));
  ASSERT_EQ(OB_SUCCESS, filter_ops.push_back(&first_filter_op));
  ASSERT_EQ(OB_SUCCESS, filter_ops.push_back(&second_filter_op));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(conjunction_param));
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      optional_bmm, score_ops, &conjunction_op, 1000, 1, query_optional));
  EXPECT_TRUE(optional_bmm.query_optional_);
  EXPECT_FALSE(optional_bmm.enable_vec_merge_);
  EXPECT_FALSE(optional_bmm.doc_score_accumulator_.is_valid());
  EXPECT_EQ(nullptr, optional_bmm.filter_bitset_);
  EXPECT_EQ(0, optional_bmm.sparse_candidate_buffer_.get_capacity());
  EXPECT_EQ(OB_SUCCESS, optional_bmm.close());
  EXPECT_EQ(OB_SUCCESS, conjunction_op.close());
}

TEST_F(TestDASBMM, sparse_seek_leapfrog_aligns_filter_and_essential_iters)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPosting token_a_postings[] = {
    {2, 1.0, 100, 5.0},
    {5, 2.0, 100, 5.0},
    {9, 3.0, 100, 5.0},
  };
  const ScriptedBMMPosting token_b_postings[] = {
    {3, 10.0, 100, 50.0},
    {5, 20.0, 100, 50.0},
    {8, 30.0, 100, 50.0},
    {9, 40.0, 100, 50.0},
  };
  const ScriptedBMMPosting filter_postings[] = {
    {1, 1000.0, 100, 1000.0},
    {5, 1000.0, 100, 1000.0},
    {7, 1000.0, 100, 1000.0},
    {9, 1000.0, 100, 1000.0},
    {12, 1000.0, 100, 1000.0},
  };
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ScriptedBMMSearchOp token_a(
      search_ctx_, token_a_postings, ARRAYSIZEOF(token_a_postings), 5.0);
  ScriptedBMMSearchOp token_b(
      search_ctx_, token_b_postings, ARRAYSIZEOF(token_b_postings), 50.0);
  ScriptedBMMSearchOp filter_op(
      search_ctx_, filter_postings, ARRAYSIZEOF(filter_postings), 1000.0);
  ASSERT_EQ(OB_SUCCESS, token_a.init(score_param));
  ASSERT_EQ(OB_SUCCESS, token_b.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_a));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token_b));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      bmm, score_ops, &filter_op, 100000, 10));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
  bmm.vec_non_essential_dim_count_ = 0;
  ObDASRowID start_id;
  start_id.set_uint64(1);
  for (int64_t i = 0; i < bmm.merge_iters_.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, bmm.merge_iters_[i]->advance_to(start_id));
  }

  bool window_end = false;
  ObDASRowID matched_id;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.find_leapfrog_candidate(matched_id, score, window_end));
  EXPECT_EQ(5, matched_id.get_uint64());
  EXPECT_DOUBLE_EQ(22.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.next_filter_rowid(bmm.curr_filter_id_));
  ASSERT_EQ(OB_SUCCESS, bmm.find_leapfrog_candidate(matched_id, score, window_end));
  EXPECT_EQ(9, matched_id.get_uint64());
  EXPECT_DOUBLE_EQ(43.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.next_filter_rowid(bmm.curr_filter_id_));
  EXPECT_EQ(OB_ITER_END, bmm.find_leapfrog_candidate(matched_id, score, window_end));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, sparse_seek_boundary_crossing_preserves_candidate)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPosting token_postings[] = {
    {5, 2.0, 100, 5.0},
    {9, 3.0, 100, 5.0},
    {12, 4.0, 100, 5.0},
  };
  const ScriptedBMMPosting filter_postings[] = {
    {5, 100.0, 100, 100.0},
    {9, 100.0, 100, 100.0},
    {12, 100.0, 100, 100.0},
  };
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ScriptedBMMSearchOp token(
      search_ctx_, token_postings, ARRAYSIZEOF(token_postings), 5.0);
  ScriptedBMMSearchOp filter_op(
      search_ctx_, filter_postings, ARRAYSIZEOF(filter_postings), 100.0);
  ASSERT_EQ(OB_SUCCESS, token.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      bmm, score_ops, &filter_op, 100000, 10));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
  bmm.vec_non_essential_dim_count_ = 0;
  bmm.min_competitive_score_ = 0.0;
  bmm.vec_boundary_id_.set_uint64(8);
  ObDASRowID start_id;
  start_id.set_uint64(1);
  ASSERT_EQ(OB_SUCCESS, bmm.merge_iters_[0]->advance_to(start_id));

  uint64_t doc_id = 0;
  ASSERT_EQ(OB_SUCCESS, bmm.collect_leapfrog_candidates());
  ASSERT_EQ(1, bmm.sparse_candidate_buffer_.get_count());
  EXPECT_TRUE(bmm.sparse_candidate_buffer_.get_doc_id(0, doc_id));
  EXPECT_EQ(5, doc_id);
  EXPECT_EQ(9, bmm.curr_filter_id_.get_uint64());

  ASSERT_EQ(OB_SUCCESS, bmm.collect_leapfrog_candidates());
  ASSERT_EQ(2, bmm.sparse_candidate_buffer_.get_count());
  EXPECT_TRUE(bmm.sparse_candidate_buffer_.get_doc_id(0, doc_id));
  EXPECT_EQ(9, doc_id);
  EXPECT_TRUE(bmm.sparse_candidate_buffer_.get_doc_id(1, doc_id));
  EXPECT_EQ(12, doc_id);
  EXPECT_TRUE(bmm.filter_end_);
  EXPECT_EQ(OB_ITER_END, bmm.collect_leapfrog_candidates());
  EXPECT_TRUE(bmm.sparse_candidate_buffer_.is_empty());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, sparse_seek_scores_non_essential_dims_and_prunes)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPosting low_dim_postings[] = {
    {30, 1.0, 100, 2.0},
  };
  const ScriptedBMMPosting high_dim_postings[] = {
    {20, 3.0, 100, 3.0},
  };
  const ScriptedBMMPosting essential_postings[] = {
    {10, 10.0, 100, 10.0},
  };
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ScriptedBMMSearchOp low_dim(
      search_ctx_, low_dim_postings, ARRAYSIZEOF(low_dim_postings), 2.0);
  ScriptedBMMSearchOp high_dim(
      search_ctx_, high_dim_postings, ARRAYSIZEOF(high_dim_postings), 3.0);
  ScriptedBMMSearchOp essential(
      search_ctx_, essential_postings, ARRAYSIZEOF(essential_postings), 10.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, nullptr, 0, 0.0);
  ASSERT_EQ(OB_SUCCESS, low_dim.init(score_param));
  ASSERT_EQ(OB_SUCCESS, high_dim.init(score_param));
  ASSERT_EQ(OB_SUCCESS, essential.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&low_dim));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&high_dim));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&essential));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      bmm, score_ops, &filter_op, 100000, 10));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
  bmm.vec_non_essential_dim_count_ = 2;
  bmm.non_ess_block_max_score_suffix_sums_.at(0) = 5.0;
  bmm.non_ess_block_max_score_suffix_sums_.at(1) = 3.0;
  bmm.min_competitive_score_ = 6.0;
  ASSERT_EQ(OB_SUCCESS, bmm.sparse_candidate_buffer_.append(10, 4.0));
  ASSERT_EQ(OB_SUCCESS, bmm.sparse_candidate_buffer_.append(20, 4.0));
  ASSERT_EQ(OB_SUCCESS, bmm.sparse_candidate_buffer_.append(30, 6.0));

  ASSERT_EQ(OB_SUCCESS, bmm.score_leapfrog_candidates());
  EXPECT_EQ(3, high_dim.get_advance_to_count());
  EXPECT_EQ(2, low_dim.get_advance_to_count());
  uint64_t doc_id = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.sparse_candidate_buffer_.next(6.0, doc_id, score));
  EXPECT_EQ(20, doc_id);
  EXPECT_DOUBLE_EQ(7.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.sparse_candidate_buffer_.next(6.0, doc_id, score));
  EXPECT_EQ(30, doc_id);
  EXPECT_DOUBLE_EQ(7.0, score);
  EXPECT_EQ(OB_ITER_END, bmm.sparse_candidate_buffer_.next(6.0, doc_id, score));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, sparse_seek_stops_scoring_after_all_candidates_are_pruned)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPosting low_dim_postings[] = {
    {100, 1.0, 100, 2.0},
  };
  const ScriptedBMMPosting high_dim_postings[] = {
    {100, 1.0, 100, 3.0},
  };
  const ScriptedBMMPosting essential_postings[] = {
    {10, 10.0, 100, 10.0},
  };
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ScriptedBMMSearchOp low_dim(
      search_ctx_, low_dim_postings, ARRAYSIZEOF(low_dim_postings), 2.0);
  ScriptedBMMSearchOp high_dim(
      search_ctx_, high_dim_postings, ARRAYSIZEOF(high_dim_postings), 3.0);
  ScriptedBMMSearchOp essential(
      search_ctx_, essential_postings, ARRAYSIZEOF(essential_postings), 10.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, nullptr, 0, 0.0);
  ASSERT_EQ(OB_SUCCESS, low_dim.init(score_param));
  ASSERT_EQ(OB_SUCCESS, high_dim.init(score_param));
  ASSERT_EQ(OB_SUCCESS, essential.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 3> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&low_dim));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&high_dim));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&essential));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      bmm, score_ops, &filter_op, 100000, 10));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.sort_iters_by_max_score());
  bmm.vec_non_essential_dim_count_ = 2;
  bmm.non_ess_block_max_score_suffix_sums_.at(0) = 5.0;
  bmm.non_ess_block_max_score_suffix_sums_.at(1) = 3.0;
  bmm.min_competitive_score_ = 6.0;
  ASSERT_EQ(OB_SUCCESS, bmm.sparse_candidate_buffer_.append(10, 1.0));
  ASSERT_EQ(OB_SUCCESS, bmm.sparse_candidate_buffer_.append(20, 2.0));

  ASSERT_EQ(OB_SUCCESS, bmm.score_leapfrog_candidates());
  EXPECT_FALSE(bmm.sparse_candidate_buffer_.has_alive());
  EXPECT_EQ(2, high_dim.get_advance_to_count());
  EXPECT_EQ(0, low_dim.get_advance_to_count());
  uint64_t doc_id = 0;
  double score = 0.0;
  EXPECT_EQ(OB_ITER_END, bmm.sparse_candidate_buffer_.next(6.0, doc_id, score));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, sparse_seek_next_advance_and_rescan)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  const ScriptedBMMPosting non_essential_postings[] = {
    {5, 1.5, 100, 2.0},
    {9, 2.0, 100, 2.0},
  };
  const ScriptedBMMPosting essential_postings[] = {
    {2, 1.0, 100, 4.0},
    {5, 2.0, 100, 4.0},
    {9, 2.5, 100, 4.0},
    {12, 4.0, 100, 4.0},
  };
  const ScriptedBMMPosting filter_postings[] = {
    {1, 100.0, 100, 100.0},
    {5, 100.0, 100, 100.0},
    {7, 100.0, 100, 100.0},
    {9, 100.0, 100, 100.0},
    {12, 100.0, 100, 100.0},
    {20, 100.0, 100, 100.0},
  };
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ScriptedBMMSearchOp non_essential(
      search_ctx_, non_essential_postings, ARRAYSIZEOF(non_essential_postings), 2.0);
  ScriptedBMMSearchOp essential(
      search_ctx_, essential_postings, ARRAYSIZEOF(essential_postings), 4.0);
  ScriptedBMMSearchOp filter_op(
      search_ctx_, filter_postings, ARRAYSIZEOF(filter_postings), 100.0);
  ASSERT_EQ(OB_SUCCESS, non_essential.init(score_param));
  ASSERT_EQ(OB_SUCCESS, essential.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&non_essential));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&essential));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      bmm, score_ops, &filter_op, 100000, 10));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(3.0));

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(5, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(3.5, score);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(rowid, rowid, score));
  EXPECT_EQ(5, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(3.5, score);
  ObDASRowID target;
  target.set_uint64(10);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(12, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(4.0, score);
  ASSERT_EQ(OB_SUCCESS, bmm.advance_to(rowid, rowid, score));
  EXPECT_EQ(12, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(4.0, score);
  target.set_uint64(20);
  EXPECT_EQ(OB_ITER_END, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(ObDASBMMOp::SearchStatus::FINISHED, bmm.status_);

  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  bmm.sparse_candidate_buffer_.reset();
  ASSERT_EQ(OB_SUCCESS, bmm.sparse_candidate_buffer_.init(2, allocator_));
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(3.0));
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(5, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(3.5, score);
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(9, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(4.5, score);
  ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
  EXPECT_EQ(12, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(4.0, score);
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, advance_to_iter_end_marks_finished)
{
  ScriptedBMMSearchOpParam token_param;
  ScriptedBMMSearchOp token(search_ctx_, 5, 10, 2.0, 2.0);
  ASSERT_EQ(OB_SUCCESS, token.init(token_param));

  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
  ObDASBMMOp bmm(search_ctx_);
  ObDASBMMOpParam bmm_param(score_ops, nullptr, allocator_, ObDASBMMBuildOptions());
  ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
  bmm.enable_vec_merge_ = false;
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(6);
  EXPECT_EQ(OB_ITER_END, bmm.advance_to(target, rowid, score));
  EXPECT_EQ(ObDASBMMOp::SearchStatus::FINISHED, bmm.status_);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, finished_threshold_update_is_noop)
{
  ScriptedBMMSearchOpParam token_param;
  ScriptedBMMSearchOp token(search_ctx_, 5, 10, 2.0, 2.0);
  ASSERT_EQ(OB_SUCCESS, token.init(token_param));

  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
  ObDASBMMOp bmm(search_ctx_);
  ObDASBMMOpParam bmm_param(score_ops, nullptr, allocator_, ObDASBMMBuildOptions());
  ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
  bmm.enable_vec_merge_ = false;
  ASSERT_EQ(OB_SUCCESS, bmm.open());

  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(5);
  ASSERT_EQ(OB_SUCCESS, bmm.merge_iters_.at(0)->advance_shallow(target, true));
  ASSERT_TRUE(bmm.merge_iters_.at(0)->in_shallow_status());
  bmm.status_ = ObDASBMMOp::SearchStatus::FINISHED;
  const int64_t calc_count = token.get_calc_max_score_count();

  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(1.0));
  EXPECT_DOUBLE_EQ(1.0, bmm.min_competitive_score_);
  EXPECT_EQ(calc_count, token.get_calc_max_score_count());
  EXPECT_TRUE(bmm.merge_iters_.at(0)->in_shallow_status());
  EXPECT_EQ(OB_INVALID_ARGUMENT, bmm.set_min_competitive_score(0.5));
  EXPECT_DOUBLE_EQ(1.0, bmm.min_competitive_score_);
  EXPECT_EQ(calc_count, token.get_calc_max_score_count());
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));

  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  EXPECT_EQ(ObDASBMMOp::SearchStatus::FIND_NEXT_PIVOT, bmm.status_);
  EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, bmm.min_competitive_score_);
  EXPECT_FALSE(bmm.merge_iters_.at(0)->in_shallow_status());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(1.0));
  EXPECT_DOUBLE_EQ(1.0, bmm.min_competitive_score_);
  EXPECT_GT(token.get_calc_max_score_count(), calc_count);
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

TEST_F(TestDASBMM, scored_batch_is_bit_exact_with_materialized_bm25)
{
  ObBM25PrecomputedParams params;
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(2.13, 67.0, 1.7, params));
  for (const int64_t capacity : {1, 64, 257, 16384}) {
    for (const uint64_t start : {uint64_t(0), UINT64_MAX - capacity + 1}) {
      DocScoreAccumulator expected_acc;
      DocScoreAccumulator actual_acc;
      ASSERT_EQ(OB_SUCCESS, expected_acc.init(capacity, allocator_));
      ASSERT_EQ(OB_SUCCESS, actual_acc.init(capacity, allocator_));
      uint64_t max_id = 0;
      ASSERT_EQ(OB_SUCCESS, expected_acc.reuse_with_start_id(start, max_id));
      ASSERT_EQ(OB_SUCCESS, actual_acc.reuse_with_start_id(start, max_id));
      uint64_t ids[513];
      uint64_t tf[513];
      uint64_t dl[513];
      double scores[513];
      for (int64_t i = 0; i < ARRAYSIZEOF(ids); ++i) {
        ids[i] = start + MIN(i / 2, capacity - 1);
        tf[i] = 1 + (i * 5) % 13;
        dl[i] = i % 17 == 0 ? UINT64_MAX : uint64_t(20 + (i * 31) % 400);
      }
      ObExprBM25::eval_batch_with_precomputed_params(tf, dl, ARRAYSIZEOF(ids), params, scores);
      for (int64_t pass = 0; pass < 3; ++pass) {
        ASSERT_EQ(OB_SUCCESS, expected_acc.add_dim_batch(ids, scores, ARRAYSIZEOF(ids)));
        int64_t calls = 0;
        const auto score_at = [&](const int64_t i) {
          ++calls;
          return ObExprBM25::eval_with_precomputed_params(tf[i], dl[i], params);
        };
        const TestBatchScorer<decltype(score_at)> scorer{score_at};
        ASSERT_EQ(OB_SUCCESS,
            actual_acc.add_scored_dim_batch(ids, ARRAYSIZEOF(ids), scorer));
        EXPECT_EQ(ARRAYSIZEOF(ids), calls);
        EXPECT_EQ(expected_acc.is_empty(), actual_acc.is_empty());
        double expected_max = expected_acc.get_max_score();
        double actual_max = actual_acc.get_max_score();
        EXPECT_EQ(0, MEMCMP(&expected_max, &actual_max, sizeof(double)));
        for (int64_t offset = 0; offset < capacity; ++offset) {
          EXPECT_EQ(expected_acc.bitset_->exist(offset), actual_acc.bitset_->exist(offset));
          if (expected_acc.bitset_->exist(offset)) {
            double expected = 0;
            double actual = 0;
            ASSERT_EQ(OB_SUCCESS, expected_acc.get_score(start + offset, expected));
            ASSERT_EQ(OB_SUCCESS, actual_acc.get_score(start + offset, actual));
            EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
          }
        }
        ASSERT_EQ(OB_SUCCESS, expected_acc.filter_below_score(1.0));
        ASSERT_EQ(OB_SUCCESS, actual_acc.filter_below_score(1.0));
      }
    }
  }
}

TEST_F(TestDASBMM, scored_batch_keeps_endpoint_validation)
{
  DocScoreAccumulator acc;
  int64_t calls = 0;
  const auto score_at = [&](const int64_t i) { ++calls; return double(i + 1); };
  const TestBatchScorer<decltype(score_at)> scorer{score_at};
  const uint64_t ids[] = {10, 11};
  const uint64_t out_of_range_ids[] = {9, 12};
  EXPECT_EQ(OB_SUCCESS, acc.add_scored_dim_batch(nullptr, 0, scorer));
  EXPECT_EQ(OB_INVALID_ARGUMENT, acc.add_scored_dim_batch(ids, 1, scorer));
  ASSERT_EQ(OB_SUCCESS, acc.init(8, allocator_));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, acc.reuse_with_start_id(10, max_id));
  EXPECT_EQ(OB_INVALID_ARGUMENT, acc.add_scored_dim_batch(nullptr, 1, scorer));
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE,
      acc.add_scored_dim_batch(out_of_range_ids, ARRAYSIZEOF(out_of_range_ids), scorer));
  EXPECT_EQ(0, calls);
  EXPECT_TRUE(acc.is_empty());
  acc.set_filter_mask(acc.bitset_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, acc.add_scored_dim_batch(ids, 2, scorer));
  EXPECT_EQ(0, calls);
  acc.clear_filter_mask();
}

TEST_F(TestDASBMM, scored_unroll_preserves_bits)
{
  ObBM25PrecomputedParams params;
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(2.13, 67.0, 1.7, params));
  const uint64_t edges[] = {0, 1, UINT32_MAX, (1ULL << 53) + 1, 1ULL << 63, UINT64_MAX};
  for (const uint64_t start : {uint64_t(0), UINT64_MAX - 256}) {
    for (const int64_t count : {0, 1, 2, 3, 31, 128, 255, 256, 513}) {
      DocScoreAccumulator expected;
      DocScoreAccumulator actual;
      ASSERT_EQ(OB_SUCCESS, expected.init(257, allocator_));
      ASSERT_EQ(OB_SUCCESS, actual.init(257, allocator_));
      uint64_t max_id = 0;
      ASSERT_EQ(OB_SUCCESS, expected.reuse_with_start_id(start, max_id));
      ASSERT_EQ(OB_SUCCESS, actual.reuse_with_start_id(start, max_id));
      uint64_t ids[513];
      for (int64_t i = 0; i < ARRAYSIZEOF(ids); ++i) {
        ids[i] = start + i / 2;
      }
      for (int64_t pass = 0; pass < 3; ++pass) {
        const auto score_at = [&](const int64_t i) {
          return ObExprBM25::eval_with_precomputed_params(
              edges[(i + pass) % ARRAYSIZEOF(edges)],
              edges[(i / ARRAYSIZEOF(edges) + pass) % ARRAYSIZEOF(edges)], params);
        };
        const TestBatchScorer<decltype(score_at)> scorer{score_at};
        double scores[ARRAYSIZEOF(ids)];
        for (int64_t i = 0; i < count; ++i) {
          scores[i] = score_at(i);
        }
        ASSERT_EQ(OB_SUCCESS, expected.add_dim_batch(ids, scores, count));
        ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch(ids, count, scorer));
        EXPECT_EQ(expected.is_empty(), actual.is_empty());
        const double expected_max = expected.get_max_score();
        const double actual_max = actual.get_max_score();
        EXPECT_EQ(0, MEMCMP(&expected_max, &actual_max, sizeof(double)));
        for (int64_t offset = 0; offset < 257; ++offset) {
          ASSERT_EQ(expected.bitset_->exist(offset), actual.bitset_->exist(offset));
          if (expected.bitset_->exist(offset)) {
            double a = 0, b = 0;
            ASSERT_EQ(OB_SUCCESS, expected.get_score(start + offset, a));
            ASSERT_EQ(OB_SUCCESS, actual.get_score(start + offset, b));
            EXPECT_EQ(0, MEMCMP(&a, &b, sizeof(double)));
          }
        }
        ASSERT_EQ(OB_SUCCESS, expected.filter_below_score(1.0));
        ASSERT_EQ(OB_SUCCESS, actual.filter_below_score(1.0));
      }
    }
  }
}

TEST_F(TestDASBMM, accumulated_drain_preserves_current_boundary_eof_and_rescan)
{
  MockBMMPosting postings[600];
  for (int64_t i = 0; i < ARRAYSIZEOF(postings); ++i) {
    postings[i] = MockBMMPosting(10 + i, double(1 + i % 7), 1000, 7.0);
  }
  for (const bool scoring : {true, false}) {
    MockBMMTokenOpParam token_param;
    token_param.set_is_scoring(scoring);
    MockBMMTokenOp token(search_ctx_, postings, ARRAYSIZEOF(postings), 7.0);
    ASSERT_EQ(OB_SUCCESS, token.init(token_param));
    ASSERT_EQ(OB_SUCCESS, token.open());
    ObDAATMergeIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(0, token));
    DocScoreAccumulator acc;
    ASSERT_EQ(OB_SUCCESS, acc.init(1024, allocator_));
    void *mask_buf = allocator_.alloc(ObBitVector::memory_size(1024));
    ASSERT_NE(nullptr, mask_buf);
    ObBitVector *mask = to_bit_vector(mask_buf);
    mask->init(1024);
    mask->reset(1024);
    for (int64_t i = 0; i < 1024; i += 2) { mask->set(i); }
    for (const bool filtered : {false, true}) {
      for (const int64_t first_count : {1, 256, 257, 513, 600}) {
        ASSERT_EQ(OB_SUCCESS, token.rescan());
        iter.reuse();
        ASSERT_EQ(OB_SUCCESS, iter.next());
        uint64_t max_id = 0;
        ASSERT_EQ(OB_SUCCESS, acc.reuse_with_start_id(10, max_id));
        if (filtered) { acc.set_filter_mask(mask); }
        else { acc.clear_filter_mask(); }
        ObDASRowID boundary;
        boundary.set_uint64(10 + first_count - 1);
        ASSERT_EQ(OB_SUCCESS, iter.drain_until(boundary, acc));
        ASSERT_EQ(OB_SUCCESS, iter.drain_until(boundary, acc));
        if (first_count < ARRAYSIZEOF(postings)) {
          EXPECT_FALSE(iter.iter_end());
          ObDASRowID next_id;
          double next_score = 0;
          ASSERT_EQ(OB_SUCCESS, iter.get_curr_id(next_id));
          ASSERT_EQ(OB_SUCCESS, iter.get_curr_score(next_score));
          EXPECT_EQ(10 + first_count, next_id.get_uint64());
          EXPECT_EQ(scoring ? postings[first_count].score_ : 0.0, next_score);
        }
        boundary.set_uint64(1000);
        ASSERT_EQ(OB_SUCCESS, iter.drain_until(boundary, acc));
        EXPECT_TRUE(iter.iter_end());
        EXPECT_FALSE(iter.curr_score_materialized_);
        for (int64_t i = 0; i < ARRAYSIZEOF(postings); ++i) {
          const bool expected = !filtered || i % 2 == 0;
          EXPECT_EQ(expected, acc.bitset_->exist(i));
          if (expected) {
            double score = 0;
            ASSERT_EQ(OB_SUCCESS, acc.get_score(10 + i, score));
            EXPECT_EQ(scoring ? postings[i].score_ : 0.0, score);
          }
        }
      }
    }
    ASSERT_EQ(OB_SUCCESS, token.close());
  }
}

TEST_F(TestDASBMM, generic_drain_preserves_duplicate_postings)
{
  MockBMMPosting postings[600];
  for (int64_t i = 0; i < ARRAYSIZEOF(postings); ++i) {
    postings[i] = MockBMMPosting(10, 1.0, 10, 1.0);
  }
  MockBMMTokenOpParam token_param;
  MockBMMTokenOp token(search_ctx_, postings, ARRAYSIZEOF(postings), 1.0);
  ASSERT_EQ(OB_SUCCESS, token.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token.open());
  ObDAATMergeIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(0, token));
  ASSERT_EQ(OB_SUCCESS, iter.next());
  DocScoreAccumulator acc;
  ASSERT_EQ(OB_SUCCESS, acc.init(1, allocator_));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, acc.reuse_with_start_id(10, max_id));
  ObDASRowID boundary;
  boundary.set_uint64(10);
  // The original generic path must preserve all duplicates, including the cached row.
  ASSERT_EQ(OB_SUCCESS, iter.drain_until(boundary, acc));
  EXPECT_TRUE(iter.iter_end());
  EXPECT_FALSE(iter.curr_score_materialized_);
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, acc.get_score(10, score));
  EXPECT_DOUBLE_EQ(600.0, score);
  EXPECT_EQ(OB_SUCCESS, token.close());
}

TEST_F(TestDASBMM, async_accumulated_drain_rejects_invalid_state)
{
  ObDASAsyncTokenOp token(search_ctx_);
  DocScoreAccumulator acc;
  ObDASRowID boundary;
  boundary.set_uint64(10);
  ObDASRowID next_id;
  double next_score = -1.0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      token.drain_until_accumulated(boundary, acc, next_id, next_score));
  EXPECT_TRUE(next_id.is_normal());
  EXPECT_EQ(0, next_id.get_uint64());
  EXPECT_EQ(0.0, next_score);
  ASSERT_EQ(OB_SUCCESS, acc.init(1, allocator_));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, acc.reuse_with_start_id(10, max_id));
  boundary.set_min();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      token.drain_until_accumulated(boundary, acc, next_id, next_score));
  boundary.set_uint64(10);
  acc.set_filter_mask(acc.bitset_);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      token.drain_until_accumulated(boundary, acc, next_id, next_score));
  acc.clear_filter_mask();
  EXPECT_EQ(OB_NOT_INIT,
      token.drain_until_accumulated(boundary, acc, next_id, next_score));
  EXPECT_TRUE(next_id.is_normal());
  EXPECT_EQ(0, next_id.get_uint64());
  EXPECT_EQ(0.0, next_score);
}

TEST_F(TestDASBMM, active_drain_fallback_positions_and_scores_current_row)
{
  ScriptedBMMSearchOpParam token_param;
  ScriptedBMMSearchOp token(search_ctx_, 5, 10, 2.0, 2.0);
  ASSERT_EQ(OB_SUCCESS, token.init(token_param));
  ASSERT_EQ(OB_SUCCESS, token.open());

  DocScoreAccumulator acc;
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, acc.init(16, allocator_));
  ASSERT_EQ(OB_SUCCESS, acc.reuse_with_start_id(0, max_id));
  ASSERT_EQ(OB_SUCCESS, acc.add_dim(5, 1.0));

  ObDASRowID boundary;
  boundary.set_uint64(10);
  ASSERT_EQ(OB_SUCCESS, token.drain_until_active(boundary, acc));

  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, acc.get_score(5, score));
  EXPECT_DOUBLE_EQ(3.0, score);
  EXPECT_EQ(OB_SUCCESS, token.close());
}

TEST_F(TestDASBMM, sparse_seek_mode_selection_uses_cost_ratio)
{
  BMMVectorizedMergeConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  ScriptedBMMSearchOpParam score_param;
  ScriptedBMMSearchOpParam filter_param;
  score_param.set_is_scoring(true);
  ScriptedBMMSearchOp score_op(search_ctx_, 5, 100, 5.0, 5.0);
  ScriptedBMMSearchOp filter_op(search_ctx_, 5, 100, 100.0, 100.0);
  ASSERT_EQ(OB_SUCCESS, score_op.init(score_param));
  ASSERT_EQ(OB_SUCCESS, filter_op.init(filter_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&score_op));

  ObDASBMMOp sparse_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      sparse_bmm, score_ops, &filter_op, 1000, 1));
  EXPECT_TRUE(sparse_bmm.enable_vec_merge_);
  EXPECT_EQ(ObDASBMMOp::FilterMergeMode::SPARSE_SEEK, sparse_bmm.filter_merge_mode_);
  EXPECT_EQ(ObDAATMergeIter::DRAIN_BATCH_SIZE,
      sparse_bmm.sparse_candidate_buffer_.get_capacity());
  EXPECT_FALSE(sparse_bmm.doc_score_accumulator_.is_valid());
  EXPECT_EQ(nullptr, sparse_bmm.filter_bitset_);
  EXPECT_EQ(OB_SUCCESS, sparse_bmm.close());

  ObDASBMMOp dense_filter_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      dense_filter_bmm, score_ops, &filter_op, 1000, 11));
  EXPECT_TRUE(dense_filter_bmm.enable_vec_merge_);
  EXPECT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW,
      dense_filter_bmm.filter_merge_mode_);
  EXPECT_TRUE(dense_filter_bmm.doc_score_accumulator_.is_valid());
  EXPECT_NE(nullptr, dense_filter_bmm.filter_bitset_);
  EXPECT_EQ(0, dense_filter_bmm.sparse_candidate_buffer_.get_capacity());
  EXPECT_EQ(OB_SUCCESS, dense_filter_bmm.close());

  ObDASBMMOp dense_no_filter_bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, init_bmm_with_costs(
      dense_no_filter_bmm, score_ops, nullptr, 1000, 1));
  EXPECT_EQ(ObDASBMMOp::FilterMergeMode::DENSE_WINDOW,
      dense_no_filter_bmm.filter_merge_mode_);
  EXPECT_TRUE(dense_no_filter_bmm.doc_score_accumulator_.is_valid());
  EXPECT_EQ(nullptr, dense_no_filter_bmm.filter_bitset_);
  EXPECT_EQ(OB_SUCCESS, dense_no_filter_bmm.close());
}

TEST_F(TestDASBMM, evaluate_pivot_loads_score_only_for_exact_dimension)
{
  const ScriptedBMMPosting postings[] = {
      ScriptedBMMPosting(10, 3.0, 15, 3.0),
      ScriptedBMMPosting(20, 5.0, 25, 5.0),
  };
  ScriptedBMMSearchOpParam token_param;
  ScriptedBMMSearchOp token(search_ctx_, postings, ARRAYSIZEOF(postings), 5.0);
  token.enable_advance_to_id();
  ASSERT_EQ(OB_SUCCESS, token.init(token_param));

  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
  ObDASBMMOp bmm(search_ctx_);
  ObDASBMMOpParam bmm_param(score_ops, nullptr, allocator_, ObDASBMMBuildOptions());
  ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  bmm.non_essential_dim_count_ = 1;

  ObDASRowID pivot_id;
  double score = 0.0;
  pivot_id.set_uint64(15);
  ASSERT_EQ(OB_SUCCESS, bmm.evaluate_pivot(pivot_id, 2.0, score));
  EXPECT_DOUBLE_EQ(2.0, score);
  EXPECT_FALSE(bmm.merge_iters_.at(0)->curr_score_materialized_);
  EXPECT_EQ(1, token.get_advance_to_id_count());
  EXPECT_EQ(0, token.get_load_curr_score_count());
  EXPECT_EQ(0, token.get_advance_to_count());

  pivot_id.set_uint64(20);
  ASSERT_EQ(OB_SUCCESS, bmm.evaluate_pivot(pivot_id, 2.0, score));
  EXPECT_DOUBLE_EQ(7.0, score);
  EXPECT_TRUE(bmm.merge_iters_.at(0)->curr_score_materialized_);
  EXPECT_EQ(2, token.get_advance_to_id_count());
  EXPECT_EQ(1, token.get_load_curr_score_count());
  EXPECT_EQ(0, token.get_advance_to_count());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
}

// Exercise cached-row ownership and batch draining around the 256-row limit and boundary/EOF edges,
// ensuring each in-range row is accumulated once, lookahead remains cached, and drain counts and
// iterator EOF state stay consistent.
TEST_F(TestDASBMM, daat_cached_current_drain_has_unique_boundary_and_eof_ownership)
{
  const auto run_case = [this](
      const int64_t posting_count,
      const uint64_t boundary_id,
      const int64_t expected_count,
      const bool expected_eof,
      const uint64_t expected_next_id,
      const int64_t expected_drain_calls) {
    ScriptedBMMPostingList postings;
    postings.reserve(posting_count);
    for (int64_t i = 0; i < posting_count; ++i) {
      postings.push_back(ScriptedBMMPosting(
          1 + static_cast<uint64_t>(i), 1.0, posting_count, 1.0));
    }
    ScriptedBMMSearchOpParam token_param;
    token_param.set_is_scoring(true);
    ScriptedBMMSearchOp token(search_ctx_, postings, 1.0);
    token.use_batch_drain(true);
    ASSERT_EQ(OB_SUCCESS, token.init(token_param));
    ASSERT_EQ(OB_SUCCESS, token.open());

    ObDAATMergeIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(0, token));
    ASSERT_EQ(OB_SUCCESS, iter.next());
    ObDASRowID cached_id;
    ASSERT_EQ(OB_SUCCESS, iter.get_curr_id(cached_id));
    ASSERT_EQ(1, cached_id.get_uint64());

    DocScoreAccumulator accumulator;
    uint64_t max_id = 0;
    ASSERT_EQ(OB_SUCCESS, accumulator.init(posting_count + 2, allocator_));
    ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(1, max_id));
    ObDASRowID boundary;
    boundary.set_uint64(boundary_id);
    ASSERT_EQ(OB_SUCCESS, iter.drain_until(boundary, accumulator));

    DocScoreIter score_iter;
    ASSERT_EQ(OB_SUCCESS, score_iter.open(accumulator));
    int64_t actual_count = 0;
    uint64_t previous_id = 0;
    uint64_t doc_id = 0;
    double score = 0.0;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = score_iter.next(doc_id, score))) {
      EXPECT_GT(doc_id, previous_id);
      EXPECT_DOUBLE_EQ(1.0, score);
      previous_id = doc_id;
      ++actual_count;
    }
    EXPECT_EQ(OB_ITER_END, ret);
    EXPECT_EQ(expected_count, actual_count);
    EXPECT_EQ(expected_eof, iter.iter_end());
    EXPECT_EQ(expected_drain_calls, token.get_call_counters().drain_);
    if (!expected_eof) {
      ObDASRowID next_id;
      ASSERT_EQ(OB_SUCCESS, iter.get_curr_id(next_id));
      EXPECT_EQ(expected_next_id, next_id.get_uint64());
      double lookahead_score = -1.0;
      ASSERT_EQ(OB_SUCCESS, accumulator.get_score(expected_next_id, lookahead_score));
      EXPECT_DOUBLE_EQ(0.0, lookahead_score);
    }
    EXPECT_EQ(OB_SUCCESS, token.close());
  };

  run_case(255, 255, 255, true, 0, 1);
  run_case(256, 256, 256, true, 0, 1);
  run_case(257, 257, 257, true, 0, 2);
  run_case(3, 1, 1, false, 2, 1);
  run_case(257, 256, 256, false, 257, 1);
}

TEST_F(TestDASBMM, simple_merger_returns_iter_end_when_active_dims_cannot_reach_msm)
{
  ScriptedBMMSearchOpParam token_param;
  ScriptedBMMSearchOp active_token(search_ctx_, 10, 100, 10.0, 10.0);
  ASSERT_EQ(OB_SUCCESS, active_token.init(token_param));
  ASSERT_EQ(OB_SUCCESS, active_token.open());

  ObDAATMergeIter active_iter;
  ASSERT_EQ(OB_SUCCESS, active_iter.init(0, active_token));
  ObSEArray<ObDAATMergeIter *, 1> merge_iters;
  ASSERT_EQ(OB_SUCCESS, merge_iters.push_back(&active_iter));

  ObIDisjunctiveDAATMerger *merger = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObIDisjunctiveDAATMerger::alloc_merger(
      merge_iters,
      rowid_exprs_,
      search_ctx_.get_rowid_meta(),
      DAS_ROWID_TYPE_UINT64,
      false,
      allocator_,
      merger));
  ASSERT_NE(nullptr, merger);
  ASSERT_EQ(OB_SUCCESS, merger->rebuild_with_next());

  ObDASRowID pivot_id;
  pivot_id.set_uint64(999);
  double max_score = 0.0;
  EXPECT_EQ(OB_ITER_END, merger->pop_until_found_pivot(1.0, 2, pivot_id, max_score));
  EXPECT_DOUBLE_EQ(10.0, max_score);
  EXPECT_EQ(999, pivot_id.get_uint64());
  EXPECT_EQ(OB_SUCCESS, active_token.close());
}

TEST_F(TestDASBMM, loser_tree_merger_returns_iter_end_when_active_dims_cannot_reach_msm)
{
  ScriptedBMMSearchOpParam token_param;
  ScriptedBMMSearchOp active_token(search_ctx_, 10, 100, 10.0, 10.0);
  EmptyScriptedBMMSearchOp empty_token_1(search_ctx_);
  EmptyScriptedBMMSearchOp empty_token_2(search_ctx_);
  EmptyScriptedBMMSearchOp empty_token_3(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, active_token.init(token_param));
  ASSERT_EQ(OB_SUCCESS, empty_token_1.init(token_param));
  ASSERT_EQ(OB_SUCCESS, empty_token_2.init(token_param));
  ASSERT_EQ(OB_SUCCESS, empty_token_3.init(token_param));
  ASSERT_EQ(OB_SUCCESS, active_token.open());
  ASSERT_EQ(OB_SUCCESS, empty_token_1.open());
  ASSERT_EQ(OB_SUCCESS, empty_token_2.open());
  ASSERT_EQ(OB_SUCCESS, empty_token_3.open());

  ObDAATMergeIter active_iter;
  ObDAATMergeIter empty_iter_1;
  ObDAATMergeIter empty_iter_2;
  ObDAATMergeIter empty_iter_3;
  ASSERT_EQ(OB_SUCCESS, active_iter.init(0, active_token));
  ASSERT_EQ(OB_SUCCESS, empty_iter_1.init(1, empty_token_1));
  ASSERT_EQ(OB_SUCCESS, empty_iter_2.init(2, empty_token_2));
  ASSERT_EQ(OB_SUCCESS, empty_iter_3.init(3, empty_token_3));
  ObSEArray<ObDAATMergeIter *, 4> merge_iters;
  ASSERT_EQ(OB_SUCCESS, merge_iters.push_back(&active_iter));
  ASSERT_EQ(OB_SUCCESS, merge_iters.push_back(&empty_iter_1));
  ASSERT_EQ(OB_SUCCESS, merge_iters.push_back(&empty_iter_2));
  ASSERT_EQ(OB_SUCCESS, merge_iters.push_back(&empty_iter_3));

  ObIDisjunctiveDAATMerger *merger = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObIDisjunctiveDAATMerger::alloc_merger(
      merge_iters,
      rowid_exprs_,
      search_ctx_.get_rowid_meta(),
      DAS_ROWID_TYPE_UINT64,
      false,
      allocator_,
      merger));
  ASSERT_NE(nullptr, merger);
  ASSERT_EQ(OB_SUCCESS, merger->rebuild_with_next());

  ObDASRowID pivot_id;
  pivot_id.set_uint64(999);
  double max_score = 0.0;
  EXPECT_EQ(OB_ITER_END, merger->pop_until_found_pivot(1.0, 2, pivot_id, max_score));
  EXPECT_DOUBLE_EQ(10.0, max_score);
  EXPECT_EQ(999, pivot_id.get_uint64());
  EXPECT_EQ(OB_SUCCESS, active_token.close());
  EXPECT_EQ(OB_SUCCESS, empty_token_1.close());
  EXPECT_EQ(OB_SUCCESS, empty_token_2.close());
  EXPECT_EQ(OB_SUCCESS, empty_token_3.close());
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

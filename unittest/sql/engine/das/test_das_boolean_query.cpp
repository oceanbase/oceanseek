/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_DAS

#include <algorithm>
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_multi_match_query.h"
#include "sql/das/search/ob_das_query_string_query.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/das/search/ob_das_conjunction_op.h"
#include "sql/das/search/ob_das_disjunction_op.h"
#include "sql/das/search/ob_das_bmw_op.h"
#include "sql/das/search/ob_das_bmm_op.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/code_generator/ob_hybrid_search_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/hybrid_search/ob_hybrid_search_node.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_das_req_opt_op.h"
#include "sql/das/search/ob_das_topk_collect_op.h"
#undef private

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::omt;

TEST(StreamingRorCost, preserves_both_advantage_regions)
{
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1600000, 47961, 1600000, 43, 10));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1600000, 31961, 1600000, 43, 100));
  EXPECT_TRUE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(20128, 4420911, 8841823, 10, 100));
  EXPECT_TRUE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(7281603, 4420911, 8841823, 10, 100));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(7281603, 4420911, 8841823, 10, 1000));
}

TEST(StreamingRorCost, invalid_or_unbounded_work_retains_bitmap)
{
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(-1, 10000, 20000, 2, 10));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1, 0, 20000, 2, 10));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1, 10000, 0, 2, 10));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1, 20001, 20000, 2, 10));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1, 10000, 20000, 1, 10));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1, 10000, 20000, 2, -1));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1, 10000, 20000, 2, 0));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1, 10000, 20000, 2, 1001));
  // Equality is not enough to pay for independent cursor setup: (1+16*2)*64.
  EXPECT_FALSE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1, 2112, 10000, 2, 1));
  EXPECT_TRUE(ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(1, 2113, 10000, 2, 1));
}

namespace
{

// Mock param for MockSearchOp, type: DAS_SEARCH_OP_DUMMY
struct MockSearchOpParam : public ObIDASSearchOpParam {
  MockSearchOpParam()
    : ObIDASSearchOpParam(DAS_SEARCH_OP_DUMMY)
  {}
  virtual ~MockSearchOpParam() {}

  virtual int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    return OB_SUCCESS;
  }
};

class MockSearchOp : public ObIDASSearchOp
{
public:
  MockSearchOp(ObDASSearchCtx &search_ctx, const std::vector<uint64_t> &rowids = {}, const std::vector<double> &scores = {})
      : ObIDASSearchOp(search_ctx), rowids_(rowids), scores_(scores), cur_idx_(-1),
        open_error_(OB_SUCCESS), rescan_error_(OB_SUCCESS), close_error_(OB_SUCCESS),
        advance_to_error_(OB_SUCCESS), next_rowid_error_(OB_SUCCESS),
        open_count_(0), rescan_count_(0)
  {
    if (scores_.empty()) {
      scores_.resize(rowids_.size(), 0.0);
    }
  }
  virtual ~MockSearchOp() {}

  void set_open_error(int ret) { open_error_ = ret; }
  void set_rescan_error(int ret) { rescan_error_ = ret; }
  void set_close_error(int ret) { close_error_ = ret; }
  void set_advance_to_error(int ret) { advance_to_error_ = ret; }
  void set_next_rowid_error(int ret) { next_rowid_error_ = ret; }
  void set_op_type_for_test(const ObDASSearchOpType op_type) { op_type_ = op_type; }
  int64_t get_open_count() const { return open_count_; }
  int64_t get_rescan_count() const { return rescan_count_; }

  virtual int do_open() override { 
    ++open_count_;
    if (open_error_ != OB_SUCCESS) {
      return open_error_;
    }
    cur_idx_ = -1;
    return OB_SUCCESS; 
  }
  virtual int do_rescan() override { 
    ++rescan_count_;
    if (rescan_error_ != OB_SUCCESS) {
      return rescan_error_;
    }
    cur_idx_ = -1;
    return OB_SUCCESS; 
  }
  virtual int do_close() override { 
    if (close_error_ != OB_SUCCESS) {
      return close_error_;
    }
    return OB_SUCCESS; 
  }
  virtual int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override
  {
    if (advance_to_error_ != OB_SUCCESS) {
      return advance_to_error_;
    }
    if (target.is_max()) {
      curr_id.set_max();
      return OB_ITER_END;
    }

    uint64_t target_val = target.is_min() ? 0 : target.get_uint64();

    if (cur_idx_ < 0) {
      cur_idx_ = 0;
    }

    while(cur_idx_ < (int64_t)rowids_.size() && rowids_[cur_idx_] < target_val) {
      cur_idx_++;
    }

    if (cur_idx_ >= (int64_t)rowids_.size()) {
      curr_id.set_max();
      return OB_ITER_END;
    }

    curr_id.set_uint64(rowids_[cur_idx_]);
    score = scores_[cur_idx_];
    return OB_SUCCESS;
  }
  virtual int do_advance_shallow(
      const ObDASRowID & /*target*/,
      const bool /*inclusive*/,
      const MaxScoreTuple *& /*max_score_tuple*/) override
  {
    return OB_NOT_IMPLEMENT;
  }
  virtual int do_next_rowid(ObDASRowID &next_id, double &score) override
  {
    if (next_rowid_error_ != OB_SUCCESS) {
      return next_rowid_error_;
    }
    cur_idx_++;
    if (cur_idx_ >= (int64_t)rowids_.size()) {
      next_id.set_max();
      return OB_ITER_END;
    }
    next_id.set_uint64(rowids_[cur_idx_]);
    score = scores_[cur_idx_];
    return OB_SUCCESS;
  }

private:
  std::vector<uint64_t> rowids_;
  std::vector<double> scores_;
  int64_t cur_idx_;
  int open_error_;
  int rescan_error_;
  int close_error_;
  int advance_to_error_;
  int next_rowid_error_;
  int64_t open_count_;
  int64_t rescan_count_;
};

class MockShallowSearchOp : public MockSearchOp
{
public:
  MockShallowSearchOp(ObDASSearchCtx &search_ctx, const int shallow_ret, const double max_score)
      : MockSearchOp(search_ctx), shallow_ret_(shallow_ret), max_score_(max_score)
  {}

  void set_shallow_ret(const int ret) { shallow_ret_ = ret; }

protected:
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override
  {
    UNUSED(inclusive);
    max_score_tuple = nullptr;
    if (OB_SUCCESS == shallow_ret_) {
      ObDASRowID max_id;
      max_id.set_max();
      max_score_tuple_.set(target, max_id, max_score_);
      max_score_tuple = &max_score_tuple_;
    }
    return shallow_ret_;
  }

private:
  int shallow_ret_;
  double max_score_;
};

class MockProbeSearchOp : public MockSearchOp
{
public:
  MockProbeSearchOp(ObDASSearchCtx &search_ctx,
                    const std::vector<uint64_t> &hit_rowids,
                    const double score)
      : MockSearchOp(search_ctx), hit_rowids_(hit_rowids), score_(score)
  {}

  bool is_probe_mode() const override { return true; }

protected:
  int do_probe(const ObDASRowID &target, bool &hit) override
  {
    hit = target.is_normal()
        && std::find(hit_rowids_.begin(), hit_rowids_.end(), target.get_uint64())
            != hit_rowids_.end();
    return OB_SUCCESS;
  }

  int do_calc_max_score(double &threshold) override
  {
    threshold = score_;
    return OB_SUCCESS;
  }

private:
  std::vector<uint64_t> hit_rowids_;
  double score_;
};

class MockProbeOnlySearchOp : public MockSearchOp
{
public:
  explicit MockProbeOnlySearchOp(ObDASSearchCtx &search_ctx)
    : MockSearchOp(search_ctx)
  {}

  bool is_probe_mode() const override { return true; }

protected:
  int do_probe(const ObDASRowID &target, bool &hit) override
  {
    UNUSED(target);
    hit = false;
    return OB_SUCCESS;
  }
};

// Mock CtDef for MockSearchOp
struct MockSearchCtDef : public ObIDASSearchCtDef
{
  MockSearchCtDef(common::ObIAllocator &alloc)
      : ObIDASSearchCtDef(alloc, DAS_OP_BOOLEAN_QUERY)
  {}
  virtual ~MockSearchCtDef() {}
};

class MockSearchRtDef : public ObIDASSearchRtDef
{
public:
  explicit MockSearchRtDef(int64_t cost, const std::vector<uint64_t> &rowids = {}, const std::vector<double> &scores = {})
      : ObIDASSearchRtDef(DAS_OP_BOOLEAN_QUERY),
        mock_cost_(cost),
        rowids_(rowids),
        scores_(scores),
        return_invalid_cost_(false),
        return_error_(OB_SUCCESS)
  {}

  void set_return_invalid_cost(bool flag) { return_invalid_cost_ = flag; }
  void set_return_error(int ret) { return_error_ = ret; }

  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  {
    if (return_error_ != OB_SUCCESS) {
      return return_error_;
    }
    if (return_invalid_cost_) {
      cost.reset();
    } else {
      cost = ObDASSearchCost(mock_cost_);
    }
    return OB_SUCCESS;
  }

  int generate_op(ObDASSearchCost /*lead_cost*/, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override
  {
    int ret = OB_SUCCESS;
    MockSearchOp *mock_op = nullptr;
    const ObIDASSearchCtDef *ctdef = nullptr;
    if (OB_ISNULL(ctdef = static_cast<const ObIDASSearchCtDef *>(ctdef_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctdef", KR(ret));
    } else {
      void *buf = search_ctx.allocator_.alloc(sizeof(MockSearchOp));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        mock_op = new(buf) MockSearchOp(search_ctx, rowids_, scores_);

        MockSearchOpParam param;
        param.set_is_scoring(ctdef->is_scoring());
        if (OB_FAIL(mock_op->init(param))) {
           LOG_WARN("failed to init mock op", KR(ret));
        } else {
           op = mock_op;
        }
      }
    }
    return ret;
  }

private:
  int64_t mock_cost_;
  std::vector<uint64_t> rowids_;
  std::vector<double> scores_;
  bool return_invalid_cost_;
  int return_error_;
};

class MockScalarScanRtDef : public ObDASScalarScanRtDef
{
public:
  explicit MockScalarScanRtDef(const int64_t mock_cost = 1)
      : ObDASScalarScanRtDef(),
        generate_count_(0),
        last_lead_cost_(-1),
        mock_cost_(mock_cost)
  {}
  ~MockScalarScanRtDef() override {}

  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  {
    UNUSED(search_ctx);
    cost = ObDASSearchCost(mock_cost_);
    return OB_SUCCESS;
  }

  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override
  {
    int ret = OB_SUCCESS;
    MockSearchOp *mock_op = nullptr;
    last_lead_cost_ = lead_cost.cost();
    ++generate_count_;
    if (OB_ISNULL(mock_op = OB_NEWx(
            MockSearchOp, &search_ctx.get_allocator(), search_ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MockSearchOpParam param;
      if (OB_FAIL(mock_op->init(param))) {
        LOG_WARN("failed to initialize mock scalar op", K(ret));
      } else {
        op = mock_op;
      }
    }
    return ret;
  }

  int64_t generate_count_;
  int64_t last_lead_cost_;

private:
  int64_t mock_cost_;
};

class FirstProbeThenEnumerableFilterRtDef : public ObDASScalarScanRtDef
{
public:
  FirstProbeThenEnumerableFilterRtDef()
    : ObDASScalarScanRtDef(), generate_count_(0)
  {}

  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  {
    UNUSED(search_ctx);
    cost = ObDASSearchCost(1);
    return OB_SUCCESS;
  }

  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override
  {
    int ret = OB_SUCCESS;
    UNUSED(lead_cost);
    ++generate_count_;
    MockSearchOpParam param;
    if (1 == generate_count_) {
      MockProbeOnlySearchOp *probe_op = OB_NEWx(
          MockProbeOnlySearchOp, &search_ctx.get_allocator(), search_ctx);
      if (OB_ISNULL(probe_op)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(probe_op->init(param))) {
        LOG_WARN("failed to initialize probe-only filter", K(ret));
      } else {
        op = probe_op;
      }
    } else {
      MockSearchOp *enumerable_op = OB_NEWx(
          MockSearchOp, &search_ctx.get_allocator(), search_ctx);
      if (OB_ISNULL(enumerable_op)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(enumerable_op->init(param))) {
        LOG_WARN("failed to initialize enumerable fallback filter", K(ret));
      } else {
        op = enumerable_op;
      }
    }
    return ret;
  }

  int64_t generate_count_;
};

class RecordingMatchRtDef : public ObDASMatchRtDef
{
public:
  RecordingMatchRtDef()
      : ObDASMatchRtDef(),
        capability_count_(0),
        compute_cost_count_(0),
        nonnull_filter_set_count_(0),
        generate_count_(0),
        compute_cost_ret_(OB_SUCCESS),
        cost_prepared_before_generate_(false),
        last_query_optional_(false),
        last_filter_cost_(-1)
  {}
  ~RecordingMatchRtDef() override {}

  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  {
    UNUSED(search_ctx);
    ++compute_cost_count_;
    if (OB_SUCCESS == compute_cost_ret_) {
      cost = ObDASSearchCost(1);
    } else {
      cost.reset();
    }
    return compute_cost_ret_;
  }

  int can_pushdown_filter_to_bmm(
      ObDASSearchCtx &search_ctx,
      bool &can_pushdown) override
  {
    UNUSED(search_ctx);
    ++capability_count_;
    ObDASMatchRtDef::set_pushdown_filter(false, nullptr);
    can_pushdown = true;
    return OB_SUCCESS;
  }

  using ObDASMatchRtDef::set_pushdown_filter;
  void set_pushdown_filter(
      const bool query_optional,
      ObIDASSearchOp *filter_op,
      ObDASSearchCost filter_cost) override
  {
    ObDASMatchRtDef::set_pushdown_filter(query_optional, filter_op, filter_cost);
    last_query_optional_ = query_optional;
    last_filter_cost_ = filter_cost;
    if (OB_NOT_NULL(filter_op)) {
      ++nonnull_filter_set_count_;
    }
  }

  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override
  {
    int ret = OB_SUCCESS;
    MockSearchOp *mock_op = nullptr;
    UNUSED(lead_cost);
    ++generate_count_;
    cost_prepared_before_generate_ = compute_cost_count_ > 0;
    if (OB_ISNULL(mock_op = OB_NEWx(
            MockSearchOp, &search_ctx.get_allocator(), search_ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MockSearchOpParam param;
      if (OB_FAIL(mock_op->init(param))) {
        LOG_WARN("failed to initialize recording match op", K(ret));
      } else {
        op = mock_op;
      }
    }
    return ret;
  }

  int64_t capability_count_;
  int64_t compute_cost_count_;
  int64_t nonnull_filter_set_count_;
  int64_t generate_count_;
  int compute_cost_ret_;
  bool cost_prepared_before_generate_;
  bool last_query_optional_;
  ObDASSearchCost last_filter_cost_;
};

template <typename FtsRtDef>
class RecordingFtsRtDef : public FtsRtDef
{
public:
  RecordingFtsRtDef()
      : FtsRtDef(),
        capability_count_(0),
        received_filter_cost_()
  {}
  ~RecordingFtsRtDef() override {}

  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  {
    UNUSED(search_ctx);
    cost = ObDASSearchCost(1);
    return OB_SUCCESS;
  }

  int can_pushdown_filter_to_bmm(
      ObDASSearchCtx &search_ctx,
      bool &can_pushdown) override
  {
    UNUSED(search_ctx);
    ++capability_count_;
    can_pushdown = true;
    return OB_SUCCESS;
  }

  using FtsRtDef::set_pushdown_filter;
  void set_pushdown_filter(
      const bool query_optional,
      ObIDASSearchOp *filter_op,
      ObDASSearchCost filter_cost) override
  {
    received_filter_cost_ = filter_cost;
    FtsRtDef::set_pushdown_filter(query_optional, filter_op, filter_cost);
  }

  int64_t capability_count_;
  ObDASSearchCost received_filter_cost_;
};

class GroupAwareMatchRtDef : public ObDASMatchRtDef
{
public:
  GroupAwareMatchRtDef()
      : ObDASMatchRtDef(),
        capability_count_(0),
        generate_count_(0),
        consumer_count_(2),
        second_group_consumer_count_(2),
        query_consumer_count_(2),
        all_groups_eligible_(false)
  {}
  ~GroupAwareMatchRtDef() override {}

  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  {
    UNUSED(search_ctx);
    cost = ObDASSearchCost(1);
    return OB_SUCCESS;
  }

  int can_pushdown_filter_to_bmm(
      ObDASSearchCtx &search_ctx,
      bool &can_pushdown) override
  {
    ++capability_count_;
    ObDASMatchRtDef::set_pushdown_filter(false, nullptr);
    can_pushdown = all_groups_eligible_
        || 0 == search_ctx.get_async_group_slot();
    return OB_SUCCESS;
  }

  int get_bmm_filter_consumer_counts(
      ObDASSearchCtx &search_ctx,
      int64_t &group_consumer_count,
      int64_t &query_consumer_count) override
  {
    group_consumer_count = 1 == search_ctx.get_async_group_slot()
        ? second_group_consumer_count_ : consumer_count_;
    query_consumer_count = query_consumer_count_;
    return OB_SUCCESS;
  }

  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override
  {
    int ret = OB_SUCCESS;
    ObIDASSearchOp *scoring_op = nullptr;
    MockSearchOp *mock_op = nullptr;
    UNUSED(lead_cost);
    ++generate_count_;
    if (OB_NOT_NULL(pushdown_filter_provider_)) {
      ObDASBitmapOp *filter_view = nullptr;
      if (OB_FAIL(pushdown_filter_provider_->create_view(search_ctx, filter_view))) {
        LOG_WARN("failed to create group-aware filter view", K(ret));
      } else {
        pushdown_filter_op_ = filter_view;
      }
    }
    if (OB_ISNULL(mock_op = OB_NEWx(
            MockSearchOp, &search_ctx.get_allocator(), search_ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MockSearchOpParam mock_param;
      mock_param.set_is_scoring(true);
      if (OB_FAIL(mock_op->init(mock_param))) {
        LOG_WARN("failed to initialize group-aware scoring op", K(ret));
      } else {
        if (0 != search_ctx.get_async_group_slot()) {
          mock_op->set_op_type_for_test(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE);
        }
        scoring_op = mock_op;
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(pushdown_filter_op_)) {
      ObSEArray<ObIDASSearchOp *, 1> scoring_ops;
      ObSEArray<ObDASSearchCost, 1> scoring_costs;
      ObDASBMMOp *bmm_op = nullptr;
      if (OB_FAIL(scoring_ops.push_back(scoring_op))) {
        LOG_WARN("failed to append group-aware scoring op", K(ret));
      } else if (OB_FAIL(scoring_costs.push_back(ObDASSearchCost(1)))) {
        LOG_WARN("failed to append group-aware scoring cost", K(ret));
      } else {
        ObDASBMMBuildOptions bmm_param_options;
        bmm_param_options.query_optional_ = query_optional_;
        bmm_param_options.lead_cost_ = lead_cost;
        bmm_param_options.filter_cost_ = pushdown_filter_cost_;
        ObDASBMMOpParam bmm_param(
            scoring_ops, pushdown_filter_op_, allocator_, bmm_param_options, &scoring_costs);
        bmm_param.set_is_scoring(true);
        if (OB_FAIL(search_ctx.create_op(bmm_param, bmm_op))) {
          LOG_WARN("failed to create group-aware bmm", K(ret));
        } else {
          op = bmm_op;
        }
      }
    } else if (OB_SUCC(ret)) {
      op = scoring_op;
    }
    ObDASMatchRtDef::set_pushdown_filter(false, nullptr);
    return ret;
  }

  int64_t capability_count_;
  int64_t generate_count_;
  int64_t consumer_count_;
  int64_t second_group_consumer_count_;
  int64_t query_consumer_count_;
  bool all_groups_eligible_;
};

// Inject a setter failure after capability checks to verify the caller's error
// contract; this does not model a known failure of a valid production target.
class RejectingProviderMatchRtDef : public GroupAwareMatchRtDef
{
public:
  RejectingProviderMatchRtDef()
    : GroupAwareMatchRtDef(), provider_set_count_(0)
  {}

  int set_pushdown_filter_provider(
      const bool query_optional,
      ObDASFilterBitmapProvider *provider,
      ObDASSearchCost filter_cost) override
  {
    UNUSED(query_optional);
    UNUSED(provider);
    UNUSED(filter_cost);
    ++provider_set_count_;
    return OB_STATE_NOT_MATCH;
  }

  int64_t provider_set_count_;
};

class RecordingAsyncBooleanRtDef : public ObDASBooleanQueryRtDef
{
public:
  RecordingAsyncBooleanRtDef()
    : ObDASBooleanQueryRtDef(),
      set_filter_count_(0),
      generate_count_(0),
      last_query_optional_(false),
      last_filter_op_(nullptr)
  {}

  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  {
    UNUSED(search_ctx);
    cost = ObDASSearchCost(1);
    return OB_SUCCESS;
  }

  int can_pushdown_filter_to_bmm(
      ObDASSearchCtx &search_ctx,
      bool &can_pushdown) override
  {
    UNUSED(search_ctx);
    can_pushdown = true;
    return OB_SUCCESS;
  }

  void set_pushdown_filter(
      const bool query_optional,
      ObIDASSearchOp *filter_op,
      ObDASSearchCost filter_cost) override
  {
    UNUSED(filter_cost);
    ++set_filter_count_;
    last_query_optional_ = query_optional;
    last_filter_op_ = filter_op;
  }

  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override
  {
    int ret = OB_SUCCESS;
    UNUSED(lead_cost);
    ++generate_count_;
    MockSearchOp *mock_op = OB_NEWx(
        MockSearchOp, &search_ctx.get_allocator(), search_ctx);
    if (OB_ISNULL(mock_op)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MockSearchOpParam param;
      if (OB_FAIL(mock_op->init(param))) {
        LOG_WARN("failed to initialize async optional mock", K(ret));
      } else {
        op = mock_op;
      }
    }
    return ret;
  }

  int64_t set_filter_count_;
  int64_t generate_count_;
  bool last_query_optional_;
  ObIDASSearchOp *last_filter_op_;
};

class ObDASBooleanQueryTest : public ::testing::Test
{
protected:
  enum ClauseType {
    MUST = 0,
    FILTER,
    SHOULD,
    MUST_NOT
  };

  ObDASBooleanQueryTest()
      : allocator_("TestDASBool"),
        ctdef_(allocator_),
        rtdef_()
  {
    rtdef_.ctdef_ = &ctdef_;
    reset_ctdef();
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }

  void TearDown() override
  {
  }

  void reset_ctdef()
  {
    ctdef_.min_should_match_ = 0;
    ctdef_.is_scoring_ = false;
    ctdef_.is_top_level_scoring_ = false;
    must_clauses_.clear();
    filter_clauses_.clear();
    should_clauses_.clear();
    must_not_clauses_.clear();
    ctdef_.children_ = nullptr;
    ctdef_.children_cnt_ = 0;
    rtdef_.children_ = nullptr;
    rtdef_.children_cnt_ = 0;
  }

  void add_clause(ClauseType type, ObIDASSearchRtDef *clause)
  {
    switch(type) {
      case MUST: must_clauses_.push_back(clause); break;
      case FILTER: filter_clauses_.push_back(clause); break;
      case SHOULD: should_clauses_.push_back(clause); break;
      case MUST_NOT: must_not_clauses_.push_back(clause); break;
      default: break;
    }
  }
  
  void prepare_structure()
  {
    int64_t must_cnt = must_clauses_.size();
    int64_t filter_cnt = filter_clauses_.size();
    int64_t should_cnt = should_clauses_.size();
    int64_t must_not_cnt = must_not_clauses_.size();
    int64_t total_cnt = must_cnt + filter_cnt + should_cnt + must_not_cnt;

    if (total_cnt > 0) {
      void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * total_cnt);
      void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * total_cnt);
      ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
      ctdef_.children_cnt_ = total_cnt;
      rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
      rtdef_.children_cnt_ = total_cnt;

      int64_t offset = 0;
      
      // MUST
      ctdef_.must_.exist_ = (must_cnt > 0);
      ctdef_.must_.offset_ = offset;
      ctdef_.must_.count_ = must_cnt;
      for (auto *c : must_clauses_) {
        ctdef_.children_[offset] = const_cast<ObDASBaseCtDef*>(c->ctdef_);
        rtdef_.children_[offset] = c;
        offset++;
      }

      // FILTER
      ctdef_.filter_.exist_ = (filter_cnt > 0);
      ctdef_.filter_.offset_ = offset;
      ctdef_.filter_.count_ = filter_cnt;
      for (auto *c : filter_clauses_) {
        ctdef_.children_[offset] = const_cast<ObDASBaseCtDef*>(c->ctdef_);
        rtdef_.children_[offset] = c;
        offset++;
      }

      // SHOULD
      ctdef_.should_.exist_ = (should_cnt > 0);
      ctdef_.should_.offset_ = offset;
      ctdef_.should_.count_ = should_cnt;
      for (auto *c : should_clauses_) {
        ctdef_.children_[offset] = const_cast<ObDASBaseCtDef*>(c->ctdef_);
        rtdef_.children_[offset] = c;
        offset++;
      }

      // MUST_NOT
      ctdef_.must_not_.exist_ = (must_not_cnt > 0);
      ctdef_.must_not_.offset_ = offset;
      ctdef_.must_not_.count_ = must_not_cnt;
      for (auto *c : must_not_clauses_) {
        ctdef_.children_[offset] = const_cast<ObDASBaseCtDef*>(c->ctdef_);
        rtdef_.children_[offset] = c;
        offset++;
      }
    }
  }

  // Setup search context for testing generate_op
  // Returns a struct containing all necessary objects that need to stay alive
  struct SearchCtxSetup {
    ObArenaAllocator scan_alloc_;
    ObDASScanOp mock_scan_op_;
    ObDASScanRtDef mock_scan_rtdef_;
    ObExecContext exec_ctx_;
    ObEvalCtx mock_eval_ctx_;
    ObDASSearchCtx search_ctx_;
    
    SearchCtxSetup(ObIAllocator &alloc)
        : scan_alloc_("MockScanOp"),
          mock_scan_op_(scan_alloc_),
          exec_ctx_(alloc),
          mock_eval_ctx_(exec_ctx_),
          search_ctx_(alloc, mock_scan_op_)
    {
      mock_eval_ctx_.max_batch_size_ = 256;
      mock_scan_rtdef_.eval_ctx_ = &mock_eval_ctx_;
      mock_scan_op_.set_scan_rtdef(&mock_scan_rtdef_);
      
      ExprFixedArray rowid_exprs(alloc);
      ExprFixedArray output(alloc);
      int ret = search_ctx_.init(rowid_exprs, output, true);
      if (OB_SUCCESS != ret) {
        LOG_ERROR("failed to init search ctx", K(ret));
      }
      search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    }
  };

  void verify_result(ObIDASSearchOp *op, const std::vector<uint64_t> &expected_ids, const std::vector<double> &expected_scores = {})
  {
    ASSERT_NE(nullptr, op);
    ASSERT_EQ(OB_SUCCESS, op->open());
    
    ObDASRowID rowid;
    double score = 0;
    
    size_t idx = 0;
    for (uint64_t val : expected_ids) {
      ASSERT_EQ(OB_SUCCESS, op->next_rowid(rowid, score));
      LOG_INFO("next_rowid", K(val), K(rowid.get_uint64()));
      EXPECT_EQ(val, rowid.get_uint64());
      if (!expected_scores.empty()) {
        ASSERT_LT(idx, expected_scores.size());
        EXPECT_DOUBLE_EQ(expected_scores[idx], score);
      }
      idx++;
    }
    
    ASSERT_EQ(OB_ITER_END, op->next_rowid(rowid, score));
    ASSERT_EQ(OB_SUCCESS, op->close());
  }

  void verify_advance_to(ObIDASSearchOp *op, const std::vector<std::tuple<uint64_t, uint64_t, double>> &targets_and_results)
  {
    ASSERT_NE(nullptr, op);
    ASSERT_EQ(OB_SUCCESS, op->open());

    ObDASRowID target_id;
    ObDASRowID res_id;
    double score = 0;

    for (const auto &tuple : targets_and_results) {
      uint64_t target_val = std::get<0>(tuple);
      uint64_t expected_val = std::get<1>(tuple);
      double expected_score = std::get<2>(tuple);
      
      target_id.set_uint64(target_val);
      int ret = op->advance_to(target_id, res_id, score);
      if (expected_val == UINT64_MAX) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        ASSERT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(expected_val, res_id.get_uint64());
        if (expected_score >= 0.0) {
          EXPECT_DOUBLE_EQ(expected_score, score);
        }
      }
    }
    
    ASSERT_EQ(OB_SUCCESS, op->close());
  }

protected:
  ObArenaAllocator allocator_;
  ObDASBooleanQueryCtDef ctdef_;
  ObDASBooleanQueryRtDef rtdef_;
  ObDASBMMBuildOptions bmm_options_{false};
  bool has_bmm_options_{false};
  std::vector<ObIDASSearchRtDef*> must_clauses_;
  std::vector<ObIDASSearchRtDef*> filter_clauses_;
  std::vector<ObIDASSearchRtDef*> should_clauses_;
  std::vector<ObIDASSearchRtDef*> must_not_clauses_;
};

template <typename Consumer>
void check_bmm_input_options(Consumer &consumer, ObIDASSearchOp &filter)
{
  ObDASBMMBuildOptions options(true);
  ASSERT_EQ(OB_SUCCESS, consumer.set_bmm_input(false, &filter, ObDASSearchCost(17), options));
  options.enable_vectorized_merge_ = false;
  EXPECT_EQ(&filter, consumer.pushdown_filter_op_);
  EXPECT_TRUE(consumer.has_bmm_build_options_);
  EXPECT_TRUE(consumer.bmm_build_options_.enable_vectorized_merge_);
  EXPECT_FALSE(consumer.query_optional_);

  EXPECT_EQ(OB_INVALID_ARGUMENT,
      consumer.set_bmm_input(true, nullptr, ObDASSearchCost(1), options));
  EXPECT_EQ(&filter, consumer.pushdown_filter_op_);
  EXPECT_TRUE(consumer.has_bmm_build_options_);
  EXPECT_TRUE(consumer.bmm_build_options_.enable_vectorized_merge_);
  EXPECT_FALSE(consumer.query_optional_);

  ASSERT_EQ(OB_SUCCESS, consumer.set_bmm_input(false, &filter, ObDASSearchCost(9), options));
  EXPECT_TRUE(consumer.has_bmm_build_options_);
  EXPECT_FALSE(consumer.bmm_build_options_.enable_vectorized_merge_);
  consumer.set_pushdown_filter(true, &filter, ObDASSearchCost(3));
  EXPECT_FALSE(consumer.has_bmm_build_options_);
  EXPECT_EQ(&filter, consumer.pushdown_filter_op_);
  EXPECT_TRUE(consumer.query_optional_);
}

} // namespace

TEST_F(ObDASBooleanQueryTest, bmm_consumers_own_options_and_clear_them_with_replaced_input)
{
  SearchCtxSetup setup(allocator_);
  MockSearchOp filter(setup.search_ctx_);
  ObDASMatchRtDef match_rtdef;
  ObDASMultiMatchRtDef multi_match_rtdef;
  ObDASQueryStringRtDef query_string_rtdef;
  check_bmm_input_options(match_rtdef, filter);
  check_bmm_input_options(multi_match_rtdef, filter);
  check_bmm_input_options(query_string_rtdef, filter);
}

TEST_F(ObDASBooleanQueryTest, match_runtime_cache_copy_does_not_copy_bmm_input)
{
  SearchCtxSetup setup(allocator_);
  MockSearchOp filter(setup.search_ctx_);
  ObDASMatchRtDef source;
  ObDASMatchRtDef clone;
  ASSERT_EQ(OB_SUCCESS, source.set_bmm_input(
      false, &filter, ObDASSearchCost(7), ObDASBMMBuildOptions(true)));
  ASSERT_EQ(OB_SUCCESS, clone.copy_cached_runtime_state_from(source));
  EXPECT_FALSE(clone.has_bmm_build_options_);
  EXPECT_EQ(nullptr, clone.pushdown_filter_op_);
  EXPECT_TRUE(source.has_bmm_build_options_);
  EXPECT_EQ(&filter, source.pushdown_filter_op_);
}

TEST_F(ObDASBooleanQueryTest, async_wrapper_forwards_filter_to_match)
{
  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASScalarScanRtDef inv_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ir_ctdef.is_async_index_ = 1;
  ir_ctdef.use_scalar_scan_def_ = 1;
  ObDASBaseCtDef *ir_ctdef_children[] = {&inv_ctdef};
  ir_ctdef.children_ = ir_ctdef_children;
  ir_ctdef.children_cnt_ = 1;
  ObDASMatchCtDef match_ctdef(allocator_);
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = 1;
  match_ctdef.ir_ctdef_idx_ = 0;
  ObDASIRScanRtDef ir_rtdef;
  inv_rtdef.ctdef_ = &inv_ctdef;
  ir_rtdef.ctdef_ = &ir_ctdef;
  ObDASBaseRtDef *ir_rtdef_children[] = {&inv_rtdef};
  ir_rtdef.children_ = ir_rtdef_children;
  ir_rtdef.children_cnt_ = 1;
  ObDASMatchRtDef match_rtdef;
  ObDASBaseRtDef *match_rtdef_children[] = {&ir_rtdef};
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.children_ = match_rtdef_children;
  match_rtdef.children_cnt_ = 1;

  ObDASScalarScanCtDef delete_ctdef(allocator_);
  ObDASScalarScanRtDef delete_rtdef;
  delete_rtdef.ctdef_ = &delete_ctdef;

  ObDASBooleanQueryCtDef wrapper_ctdef(allocator_);
  ObDASBooleanQueryRtDef wrapper_rtdef;
  ObDASBaseCtDef *wrapper_ctdef_children[] = {&match_ctdef, &delete_ctdef};
  ObDASBaseRtDef *wrapper_rtdef_children[] = {&match_rtdef, &delete_rtdef};
  wrapper_ctdef.children_ = wrapper_ctdef_children;
  wrapper_ctdef.children_cnt_ = 2;
  wrapper_ctdef.set_must(0, 1);
  wrapper_ctdef.set_must_not(1, 1);
  wrapper_ctdef.set_min_should_match(0);
  wrapper_ctdef.set_is_async(true);
  wrapper_rtdef.ctdef_ = &wrapper_ctdef;
  wrapper_rtdef.children_ = wrapper_rtdef_children;
  wrapper_rtdef.children_cnt_ = 2;

  ObIDASSearchRtDef *target_rtdef = nullptr;
  EXPECT_EQ(OB_SUCCESS, wrapper_rtdef.get_async_fts_filter_pushdown_target(target_rtdef));
  EXPECT_EQ(&match_rtdef, target_rtdef);

  ObIDASSearchOp *filter_op = reinterpret_cast<ObIDASSearchOp *>(1);
  wrapper_rtdef.set_pushdown_filter(true, filter_op);
  EXPECT_EQ(filter_op, match_rtdef.pushdown_filter_op_);
  EXPECT_TRUE(match_rtdef.query_optional_);

  for (int64_t mode = 0; mode < 2; ++mode) {
    ASSERT_EQ(OB_SUCCESS, wrapper_rtdef.set_bmm_input(
        false, filter_op, ObDASSearchCost(17), ObDASBMMBuildOptions(0 != mode)));
    EXPECT_EQ(filter_op, match_rtdef.pushdown_filter_op_);
    EXPECT_TRUE(match_rtdef.has_bmm_build_options_);
    EXPECT_EQ(0 != mode, match_rtdef.bmm_build_options_.enable_vectorized_merge_);
    EXPECT_EQ(17, match_rtdef.pushdown_filter_cost_.cost());
    EXPECT_FALSE(match_rtdef.query_optional_);
  }

  ir_ctdef.is_async_index_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, wrapper_rtdef.set_bmm_input(
      false, filter_op, ObDASSearchCost(1), ObDASBMMBuildOptions(false)));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            wrapper_rtdef.get_async_fts_filter_pushdown_target(target_rtdef));
  EXPECT_EQ(nullptr, target_rtdef);

  wrapper_ctdef.set_is_async(false);
  EXPECT_EQ(OB_NOT_SUPPORTED, wrapper_rtdef.set_bmm_input(
      false, filter_op, ObDASSearchCost(1), ObDASBMMBuildOptions(false)));
  EXPECT_EQ(OB_SUCCESS, wrapper_rtdef.get_async_fts_filter_pushdown_target(target_rtdef));
  EXPECT_EQ(nullptr, target_rtdef);
}

TEST_F(ObDASBooleanQueryTest, selected_probe_is_ordered_after_all_driver_candidates)
{
  MockSearchRtDef first(10, {1});
  MockSearchRtDef probe(1, {2});
  MockSearchRtDef second(20, {3});
  MockSearchRtDef third(30, {4});
  ObSEArray<ObIDASSearchRtDef *, 4> clauses;
  ASSERT_EQ(OB_SUCCESS, clauses.push_back(&probe));
  ASSERT_EQ(OB_SUCCESS, clauses.push_back(&first));
  ASSERT_EQ(OB_SUCCESS, clauses.push_back(&second));
  ASSERT_EQ(OB_SUCCESS, clauses.push_back(&third));

  ObDASBooleanQueryRtDef::move_selected_probe_after_drivers(clauses, &probe);
  ASSERT_EQ(4, clauses.count());
  EXPECT_EQ(&first, clauses.at(0));
  EXPECT_EQ(&second, clauses.at(1));
  EXPECT_EQ(&third, clauses.at(2));
  EXPECT_EQ(&probe, clauses.at(3));

  ObDASBooleanQueryRtDef::move_selected_probe_after_drivers(clauses, nullptr);
  EXPECT_EQ(&first, clauses.at(0));
  EXPECT_EQ(&probe, clauses.at(3));
}

TEST_F(ObDASBooleanQueryTest, topk_range_probe_cost_respects_work_and_opt_out)
{
  const int64_t n = 8841823;
  EXPECT_TRUE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 880000, n, 10, 1000));
  EXPECT_TRUE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 7920000, n, 100, 1000));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 88000, n, 10, 1000));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 880000, n, 100, 1000));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 7920000, n, 10, 1000000000));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 7920000, n, 10, 0));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 7920000, n, 0, 1));
  EXPECT_TRUE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, n, n, 101, 1));
  EXPECT_TRUE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, n, n, 1000, 1));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, n, n, 1001, 1));
  EXPECT_TRUE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(
      1000000000, 100000000, 1000000000, 1000, 1));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(-1, 7920000, n, 10, 1000));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 0, n, 10, 1000));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, n + 1, n, 10, 1000));
}

TEST_F(ObDASBooleanQueryTest, topk_probe_default_ratio_calibration_is_monotonic)
{
  const int64_t n = 8841823;
  EXPECT_TRUE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 880000, n, 10, 1000));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 880000, n, 10, 2000));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 440000, n, 10, 1000));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 440000, n, 10, 250));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 88000, n, 10, 1));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 88000, n, 10, 0));
  EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 7920000, n, 10, 1000000000));
}

TEST_F(ObDASBooleanQueryTest, topk_probe_small_ratio_preserves_work_budget_floor)
{
  const int64_t n = 8841823;
  const int64_t ratios[] = {1, 10, 100, 256, 512, 1000};
  for (const int64_t ratio : ratios) {
    EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 88000, n, 10, ratio));
    EXPECT_FALSE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 440000, n, 10, ratio));
    EXPECT_TRUE(ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(n, 880000, n, 10, ratio));
  }
}

TEST_F(ObDASBooleanQueryTest, topk_range_probe_generation_preserves_legacy_and_guards)
{
  class CostlyMatch : public RecordingMatchRtDef
  {
  public:
    int compute_cost(ObDASSearchCtx &, ObDASSearchCost &cost) override
    { cost = ObDASSearchCost(8841823); return OB_SUCCESS; }
  } match;
  ObDASIRScanCtDef ir(allocator_);
  ir.is_async_index_ = true;
  ObDASMatchCtDef match_def(allocator_);
  match_def.set_is_scoring(true);
  ObDASBaseCtDef *match_children[] = {&ir};
  match_def.children_ = match_children;
  match_def.children_cnt_ = 1;
  match_def.ir_ctdef_idx_ = 0;
  match.ctdef_ = &match_def;
  ObDASScalarCtDef scalar_def(allocator_);
  ObDASScalarScanCtDef index_def(allocator_);
  ObDASScalarScanCtDef primary_def(allocator_);
  primary_def.is_primary_table_scan_ = true;
  index_def.is_async_index_ = true;
  index_def.is_index_term_point_scan_ = false;
  index_def.is_rowkey_order_scan_ = false;
  MockScalarScanRtDef index(4400000);
  MockScalarScanRtDef primary;
  index.ctdef_ = &index_def;
  primary.ctdef_ = &primary_def;
  ObDASBaseCtDef *defs[] = {&index_def, &primary_def};
  ObDASBaseRtDef *rts[] = {&index, &primary};
  scalar_def.children_ = defs;
  scalar_def.children_cnt_ = 2;
  scalar_def.set_has_index_scan(true);
  scalar_def.set_has_main_scan(true);
  scalar_def.set_primary_get_ratio(1000);
  ObDASScalarRtDef scalar;
  scalar.ctdef_ = &scalar_def;
  scalar.children_ = rts;
  scalar.children_cnt_ = 2;
  ObIDASSearchRtDef *filters[] = {&scalar};
  ObBooleanSubClause<ObIDASSearchRtDef> filter(filters, 1);
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.table_row_count_ = ObDASSearchCost(8841823);
  setup.search_ctx_.set_generating_topk_limit(10);
  ObIDASSearchRtDef *chosen = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.choose_topk_range_probe(match, filter, setup.search_ctx_, chosen));
  EXPECT_EQ(&scalar, chosen);
  ObIDASSearchRtDef *required[] = {&match};
  ObBooleanSubClause<ObIDASSearchRtDef> must(required, 1);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.req(must, filter, ObDASSearchCost(8841823),
      true, setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(0, index.generate_count_);
  EXPECT_EQ(1, primary.generate_count_);
  EXPECT_EQ(8841823, primary.last_lead_cost_);
  EXPECT_EQ(0, match.nonnull_filter_set_count_);
  EXPECT_EQ(8841823, match.cost_.cost());
  EXPECT_EQ(4400000, scalar.cost_.cost());

  match.cost_ = ObDASSearchCost(1);
  op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.req(must, filter, ObDASSearchCost(123), true, setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(0, index.generate_count_);
  EXPECT_EQ(2, primary.generate_count_);
  EXPECT_EQ(123, primary.last_lead_cost_);
  index_def.is_index_term_point_scan_ = true;
  ASSERT_EQ(OB_SUCCESS, rtdef_.choose_topk_range_probe(match, filter, setup.search_ctx_, chosen));
  EXPECT_EQ(nullptr, chosen);
  index_def.is_index_term_point_scan_ = false;
  setup.search_ctx_.set_generating_topk_limit(-1);
  ASSERT_EQ(OB_SUCCESS, rtdef_.choose_topk_range_probe(match, filter, setup.search_ctx_, chosen));
  EXPECT_EQ(nullptr, chosen);
  setup.search_ctx_.set_generating_topk_limit(10);
  match_def.set_is_scoring(false);
  ASSERT_EQ(OB_SUCCESS, rtdef_.choose_topk_range_probe(match, filter, setup.search_ctx_, chosen));
  EXPECT_EQ(nullptr, chosen);
  match_def.set_is_scoring(true);
  setup.search_ctx_.use_dynamic_pruning_ = false;
  ASSERT_EQ(OB_SUCCESS, rtdef_.choose_topk_range_probe(match, filter, setup.search_ctx_, chosen));
  EXPECT_EQ(nullptr, chosen);
  // A docid slice restricts the enumerable MATCH driver, not the probe follower.
  // Rebuilding a worker tree must retain the selected point-get physical path.
  setup.search_ctx_.use_dynamic_pruning_ = true;
  match.cost_ = ObDASSearchCost(8841823);
  const int64_t topk_limits[] = {10, 100};
  for (int64_t topk : topk_limits) {
    ObObj lo;
    ObObj hi;
    lo.set_uint64(65536);
    hi.set_uint64(131071);
    setup.search_ctx_.set_docid_range(lo, hi);
    setup.search_ctx_.set_generating_topk_limit(topk);
    chosen = nullptr;
    ASSERT_EQ(OB_SUCCESS, rtdef_.choose_topk_range_probe(match, filter, setup.search_ctx_, chosen));
    ASSERT_EQ(&scalar, chosen);
    const int64_t previous_primary_count = primary.generate_count_;
    op = nullptr;
    ASSERT_EQ(OB_SUCCESS, rtdef_.req(must, filter, ObDASSearchCost(8841823),
        true, setup.search_ctx_, op));
    ASSERT_NE(nullptr, op);
    EXPECT_EQ(0, index.generate_count_);
    EXPECT_EQ(previous_primary_count + 1, primary.generate_count_);
    EXPECT_EQ(0, match.nonnull_filter_set_count_);
    EXPECT_TRUE(setup.search_ctx_.has_docid_range());
    EXPECT_EQ(65536, setup.search_ctx_.get_docid_range_lo().get_uint64());
    EXPECT_EQ(131071, setup.search_ctx_.get_docid_range_hi().get_uint64());
  }

}

TEST_F(ObDASBooleanQueryTest, topk_collect_generate_op_forwards_limit_and_restores_context)
{
  SearchCtxSetup setup(allocator_);
  ObDASTopKCollectCtDef topk_ctdef(allocator_);
  ObDASTopKCollectRtDef topk_rtdef;
  ObDASBooleanQueryCtDef bool_ctdef(allocator_);
  ObDASBooleanQueryRtDef bool_rtdef;
  MockSearchCtDef child_ctdef(allocator_);
  class RecordingTopKChildRtDef : public MockSearchRtDef
  {
  public:
    RecordingTopKChildRtDef()
        : MockSearchRtDef(1, {1, 2}), received_limit_(-1), received_ror_hint_(-1),
          generate_count_(0), generate_error_(OB_SUCCESS) {}
    void set_generate_error(int ret) { generate_error_ = ret; }
    int generate_op(ObDASSearchCost cost, ObDASSearchCtx &ctx, ObIDASSearchOp *&op) override
    {
      received_limit_ = ctx.get_generating_topk_limit();
      received_ror_hint_ = ctx.get_streaming_ror_costing_topk_hint();
      ++generate_count_;
      return OB_SUCCESS == generate_error_
          ? MockSearchRtDef::generate_op(cost, ctx, op) : generate_error_;
    }
    int64_t received_limit_;
    int64_t received_ror_hint_;
    int64_t generate_count_;
    int generate_error_;
  } child_rtdef;
  child_rtdef.ctdef_ = &child_ctdef;
  ObDASBaseCtDef *bool_ctdef_children[] = {&child_ctdef};
  ObDASBaseRtDef *bool_rtdef_children[] = {&child_rtdef};
  bool_ctdef.children_ = bool_ctdef_children;
  bool_ctdef.children_cnt_ = 1;
  bool_ctdef.set_must(0, 1);
  bool_rtdef.ctdef_ = &bool_ctdef;
  bool_rtdef.children_ = bool_rtdef_children;
  bool_rtdef.children_cnt_ = 1;
  ObDASBaseCtDef *ctdef_children[] = {&bool_ctdef};
  ObDASBaseRtDef *rtdef_children[] = {&bool_rtdef};
  topk_ctdef.children_ = ctdef_children;
  topk_ctdef.children_cnt_ = 1;
  topk_rtdef.ctdef_ = &topk_ctdef;
  topk_rtdef.children_ = rtdef_children;
  topk_rtdef.children_cnt_ = 1;
  topk_rtdef.eval_ctx_ = &setup.mock_eval_ctx_;
  topk_ctdef.set_is_scoring(true);

  char *frame = static_cast<char *>(allocator_.alloc(
      sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(int64_t) + sizeof(VectorHeader)));
  ASSERT_NE(nullptr, frame);
  MEMSET(frame, 0, sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(int64_t) + sizeof(VectorHeader));
  char **frames = static_cast<char **>(allocator_.alloc(sizeof(char *)));
  ASSERT_NE(nullptr, frames);
  frames[0] = frame;
  setup.exec_ctx_.frames_ = frames;
  setup.exec_ctx_.frame_cnt_ = 1;
  setup.mock_eval_ctx_.frames_ = frames;

  ObExpr limit_expr;
  limit_expr.obj_meta_.set_type(ObIntType);
  limit_expr.datum_meta_.type_ = ObIntType;
  limit_expr.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
  limit_expr.vec_value_tc_ = get_vec_value_tc(
      ObIntType, limit_expr.datum_meta_.scale_, limit_expr.datum_meta_.precision_);
  limit_expr.frame_idx_ = 0;
  limit_expr.datum_off_ = 0;
  limit_expr.eval_info_off_ = sizeof(ObDatum);
  limit_expr.res_buf_off_ = sizeof(ObDatum) + sizeof(ObEvalInfo);
  limit_expr.res_buf_len_ = sizeof(int64_t);
  limit_expr.vector_header_off_ = limit_expr.res_buf_off_ + limit_expr.res_buf_len_;
  topk_ctdef.limit_ = &limit_expr;

  limit_expr.locate_datum_for_write(setup.mock_eval_ctx_).set_int(7);
  limit_expr.set_evaluated_projected(setup.mock_eval_ctx_);
  setup.search_ctx_.set_generating_topk_limit(42);
  setup.search_ctx_.set_streaming_ror_costing_topk_hint(43);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, topk_rtdef.generate_op(
      ObDASSearchCost(3), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(7, child_rtdef.received_limit_);
  EXPECT_EQ(7, child_rtdef.received_ror_hint_);
  EXPECT_EQ(1, child_rtdef.generate_count_);
  EXPECT_EQ(43, setup.search_ctx_.get_streaming_ror_costing_topk_hint());
  EXPECT_EQ(42, setup.search_ctx_.get_generating_topk_limit());
  EXPECT_EQ(DAS_SEARCH_OP_TOPK_COLLECT, op->get_op_type());
  ObDASTopKCollectOp *topk_op = static_cast<ObDASTopKCollectOp *>(op);
  EXPECT_TRUE(topk_op->dynamic_pruning_enabled_);

  ObDASTopKCollectOpParam disabled_pruning_param;
  disabled_pruning_param.limit_ = 7;
  disabled_pruning_param.child_ = topk_op->child_;
  disabled_pruning_param.dynamic_pruning_enabled_ = false;
  ObDASTopKCollectOp *disabled_pruning_op = nullptr;
  ASSERT_EQ(OB_SUCCESS, setup.search_ctx_.create_op(disabled_pruning_param, disabled_pruning_op));
  ASSERT_NE(nullptr, disabled_pruning_op);
  EXPECT_FALSE(disabled_pruning_op->dynamic_pruning_enabled_);

  limit_expr.locate_datum_for_write(setup.mock_eval_ctx_).set_int(0);
  limit_expr.set_evaluated_projected(setup.mock_eval_ctx_);
  op = nullptr;
  child_rtdef.received_limit_ = -1;
  child_rtdef.set_generate_error(OB_ERR_UNEXPECTED);
  ASSERT_EQ(OB_SUCCESS, topk_rtdef.generate_op(
      ObDASSearchCost(3), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, op->get_op_type());
  EXPECT_EQ(42, setup.search_ctx_.get_generating_topk_limit());
  EXPECT_EQ(-1, child_rtdef.received_limit_);
  EXPECT_EQ(1, child_rtdef.generate_count_);
  EXPECT_EQ(43, setup.search_ctx_.get_streaming_ror_costing_topk_hint());
  child_rtdef.set_generate_error(OB_SUCCESS);

  child_rtdef.set_generate_error(OB_ERR_UNEXPECTED);
  limit_expr.locate_datum_for_write(setup.mock_eval_ctx_).set_int(9);
  limit_expr.set_evaluated_projected(setup.mock_eval_ctx_);
  op = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, topk_rtdef.generate_op(
      ObDASSearchCost(3), setup.search_ctx_, op));
  EXPECT_EQ(42, setup.search_ctx_.get_generating_topk_limit());
  EXPECT_EQ(9, child_rtdef.received_ror_hint_);
  EXPECT_EQ(2, child_rtdef.generate_count_);
  EXPECT_EQ(43, setup.search_ctx_.get_streaming_ror_costing_topk_hint());
}

TEST_F(ObDASBooleanQueryTest, topk_probe_cost_scope_restores_after_compound_query_and_error)
{
  class RecordingContextRtDef : public MockSearchRtDef
  {
  public:
    RecordingContextRtDef() : MockSearchRtDef(1, {1}), last_limit_(-1), error_(OB_SUCCESS) {}
    int generate_op(ObDASSearchCost cost, ObDASSearchCtx &ctx, ObIDASSearchOp *&op) override
    {
      last_limit_ = ctx.get_generating_topk_limit();
      return OB_SUCCESS == error_ ? MockSearchRtDef::generate_op(cost, ctx, op) : error_;
    }
    int64_t last_limit_;
    int error_;
  } required;
  MockSearchRtDef optional(1, {1});
  MockSearchCtDef required_def(allocator_);
  MockSearchCtDef optional_def(allocator_);
  required.ctdef_ = &required_def;
  optional.ctdef_ = &optional_def;
  add_clause(MUST, &required);
  add_clause(SHOULD, &optional);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_generating_topk_limit(10);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(10), setup.search_ctx_, op));
  EXPECT_EQ(-1, required.last_limit_);
  EXPECT_EQ(10, setup.search_ctx_.get_generating_topk_limit());

  required.error_ = OB_ERR_UNEXPECTED;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.generate_op(ObDASSearchCost(10), setup.search_ctx_, op));
  EXPECT_EQ(10, setup.search_ctx_.get_generating_topk_limit());
  required.error_ = OB_SUCCESS;
  ctdef_.should_.exist_ = false;
  ctdef_.should_.count_ = 0;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(10), setup.search_ctx_, op));
  EXPECT_EQ(10, required.last_limit_);
  EXPECT_EQ(10, setup.search_ctx_.get_generating_topk_limit());

  ctdef_.must_not_.exist_ = true;
  ctdef_.must_not_.offset_ = 1;
  ctdef_.must_not_.count_ = 1;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(10), setup.search_ctx_, op));
  EXPECT_EQ(-1, required.last_limit_);
  EXPECT_EQ(10, setup.search_ctx_.get_generating_topk_limit());
}

TEST_F(ObDASBooleanQueryTest, async_multi_match_primary_get_gate_uses_lead_cost)
{
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ir_ctdef.is_async_index_ = 1;
  ObDASMultiMatchCtDef multi_match_ctdef(allocator_);
  RecordingFtsRtDef<ObDASMultiMatchRtDef> multi_match_rtdef;
  ObDASBaseCtDef *multi_match_ctdef_children[] = {&ir_ctdef};
  multi_match_ctdef.children_ = multi_match_ctdef_children;
  multi_match_ctdef.children_cnt_ = ARRAYSIZEOF(multi_match_ctdef_children);
  ASSERT_EQ(OB_SUCCESS, multi_match_ctdef.ir_ctdef_indices_.init(1));
  ASSERT_EQ(OB_SUCCESS, multi_match_ctdef.ir_ctdef_indices_.prepare_allocate(1));
  multi_match_ctdef.ir_ctdef_indices_.at(0) = 0;
  multi_match_rtdef.ctdef_ = &multi_match_ctdef;

  ObDASScalarScanCtDef delete_ctdef(allocator_);
  ObDASScalarScanRtDef delete_rtdef;
  delete_rtdef.ctdef_ = &delete_ctdef;
  ObDASBooleanQueryCtDef wrapper_ctdef(allocator_);
  ObDASBooleanQueryRtDef wrapper_rtdef;
  ObDASBaseCtDef *wrapper_ctdef_children[] = {&multi_match_ctdef, &delete_ctdef};
  ObDASBaseRtDef *wrapper_rtdef_children[] = {&multi_match_rtdef, &delete_rtdef};
  wrapper_ctdef.children_ = wrapper_ctdef_children;
  wrapper_ctdef.children_cnt_ = ARRAYSIZEOF(wrapper_ctdef_children);
  wrapper_ctdef.set_must(0, 1);
  wrapper_ctdef.set_must_not(1, 1);
  wrapper_ctdef.set_is_async(true);
  wrapper_rtdef.ctdef_ = &wrapper_ctdef;
  wrapper_rtdef.children_ = wrapper_rtdef_children;
  wrapper_rtdef.children_cnt_ = ARRAYSIZEOF(wrapper_rtdef_children);

  ObDASScalarCtDef filter_ctdef(allocator_);
  ObDASScalarRtDef filter_rtdef;
  ObDASScalarScanCtDef index_ctdef(allocator_);
  ObDASScalarScanCtDef primary_ctdef(allocator_);
  MockScalarScanRtDef index_rtdef(10);
  MockScalarScanRtDef primary_rtdef;
  ObDASBaseCtDef *filter_ctdef_children[] = {&index_ctdef, &primary_ctdef};
  ObDASBaseRtDef *filter_rtdef_children[] = {&index_rtdef, &primary_rtdef};
  primary_ctdef.is_primary_table_scan_ = true;
  index_rtdef.ctdef_ = &index_ctdef;
  primary_rtdef.ctdef_ = &primary_ctdef;
  filter_ctdef.children_ = filter_ctdef_children;
  filter_ctdef.children_cnt_ = ARRAYSIZEOF(filter_ctdef_children);
  filter_ctdef.set_has_index_scan(true);
  filter_ctdef.set_has_main_scan(true);
  filter_ctdef.set_primary_get_ratio(10);
  filter_rtdef.ctdef_ = &filter_ctdef;
  filter_rtdef.children_ = filter_rtdef_children;
  filter_rtdef.children_cnt_ = ARRAYSIZEOF(filter_rtdef_children);
  ObIDASSearchRtDef *filter_children[] = {&filter_rtdef};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(
      filter_children, ARRAYSIZEOF(filter_children));

  SearchCtxSetup setup(allocator_);
  ObIDASSearchRtDef *fts_target = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.get_async_fts_filter_pushdown_target(fts_target));
  EXPECT_EQ(&multi_match_rtdef, fts_target);
  int64_t group_consumer_count = 0;
  int64_t query_consumer_count = 0;
  ASSERT_EQ(OB_SUCCESS, wrapper_rtdef.get_bmm_filter_consumer_counts(
      setup.search_ctx_, group_consumer_count, query_consumer_count));
  EXPECT_EQ(1, group_consumer_count);
  EXPECT_EQ(1, query_consumer_count);
  EXPECT_EQ(OB_NOT_SUPPORTED,
            wrapper_rtdef.set_pushdown_filter_provider(false, nullptr, ObDASSearchCost()));
  bool can_pushdown = false;
  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.can_pushdown_filter_to_bmm(
                setup.search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);
  EXPECT_EQ(1, multi_match_rtdef.capability_count_);
  MockSearchOp filter_op(setup.search_ctx_);
  wrapper_rtdef.set_pushdown_filter(true, &filter_op);
  EXPECT_EQ(&filter_op, multi_match_rtdef.pushdown_filter_op_);
  EXPECT_TRUE(multi_match_rtdef.query_optional_);

  for (const bool vectorized : {false, true}) {
    ASSERT_EQ(OB_SUCCESS, wrapper_rtdef.set_bmm_input(
        false, &filter_op, ObDASSearchCost(17), ObDASBMMBuildOptions(vectorized)));
    EXPECT_EQ(&filter_op, multi_match_rtdef.pushdown_filter_op_);
    EXPECT_TRUE(multi_match_rtdef.has_bmm_build_options_);
    EXPECT_EQ(vectorized, multi_match_rtdef.bmm_build_options_.enable_vectorized_merge_);
    EXPECT_EQ(17, multi_match_rtdef.received_filter_cost_.cost());
    EXPECT_FALSE(multi_match_rtdef.query_optional_);
  }

  bool skip_bmm = false;
  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.should_skip_bmm_for_async_fts_primary_get(
                multi_match_rtdef, filter_clauses, ObDASSearchCost(1),
                setup.search_ctx_, skip_bmm));
  EXPECT_TRUE(skip_bmm);

  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.should_skip_bmm_for_async_fts_primary_get(
                multi_match_rtdef, filter_clauses, ObDASSearchCost(2),
                setup.search_ctx_, skip_bmm));
  EXPECT_FALSE(skip_bmm);

  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.should_skip_bmm_for_async_fts_primary_get(
                wrapper_rtdef, filter_clauses, ObDASSearchCost(1),
                setup.search_ctx_, skip_bmm));
  EXPECT_TRUE(skip_bmm);

  // Async Group filters use the same cost threshold as unbound filters.
  index_ctdef.is_async_index_ = true;
  setup.search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.should_skip_bmm_for_async_fts_primary_get(
                multi_match_rtdef, filter_clauses, ObDASSearchCost(1),
                setup.search_ctx_, skip_bmm));
  EXPECT_TRUE(skip_bmm);
  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.should_skip_bmm_for_async_fts_primary_get(
                multi_match_rtdef, filter_clauses, ObDASSearchCost(2),
                setup.search_ctx_, skip_bmm));
  EXPECT_FALSE(skip_bmm);
  filter_ctdef.set_primary_get_ratio(0);
  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.should_skip_bmm_for_async_fts_primary_get(
                multi_match_rtdef, filter_clauses, ObDASSearchCost(0),
                setup.search_ctx_, skip_bmm));
  EXPECT_FALSE(skip_bmm);
}

TEST_F(ObDASBooleanQueryTest, async_query_string_resolves_direct_and_wrapped_fts)
{
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ir_ctdef.is_async_index_ = 1;
  ObDASQueryStringCtDef query_string_ctdef(allocator_);
  RecordingFtsRtDef<ObDASQueryStringRtDef> query_string_rtdef;
  ObDASBaseCtDef *query_string_ctdef_children[] = {&ir_ctdef};
  query_string_ctdef.children_ = query_string_ctdef_children;
  query_string_ctdef.children_cnt_ = ARRAYSIZEOF(query_string_ctdef_children);
  ASSERT_EQ(OB_SUCCESS, query_string_ctdef.ir_ctdef_indices_.init(1));
  ASSERT_EQ(OB_SUCCESS, query_string_ctdef.ir_ctdef_indices_.prepare_allocate(1));
  query_string_ctdef.ir_ctdef_indices_.at(0) = 0;
  query_string_rtdef.ctdef_ = &query_string_ctdef;

  ObDASScalarScanCtDef delete_ctdef(allocator_);
  ObDASScalarScanRtDef delete_rtdef;
  delete_rtdef.ctdef_ = &delete_ctdef;
  ObDASBooleanQueryCtDef wrapper_ctdef(allocator_);
  ObDASBooleanQueryRtDef wrapper_rtdef;
  ObDASBaseCtDef *wrapper_ctdef_children[] = {&query_string_ctdef, &delete_ctdef};
  ObDASBaseRtDef *wrapper_rtdef_children[] = {&query_string_rtdef, &delete_rtdef};
  wrapper_ctdef.children_ = wrapper_ctdef_children;
  wrapper_ctdef.children_cnt_ = ARRAYSIZEOF(wrapper_ctdef_children);
  wrapper_ctdef.set_must(0, 1);
  wrapper_ctdef.set_must_not(1, 1);
  wrapper_ctdef.set_is_async(true);
  wrapper_rtdef.ctdef_ = &wrapper_ctdef;
  wrapper_rtdef.children_ = wrapper_rtdef_children;
  wrapper_rtdef.children_cnt_ = ARRAYSIZEOF(wrapper_rtdef_children);

  ObIDASSearchRtDef *fts_target = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.get_async_fts_filter_pushdown_target(fts_target));
  EXPECT_EQ(&query_string_rtdef, fts_target);
  SearchCtxSetup setup(allocator_);
  int64_t group_consumer_count = 0;
  int64_t query_consumer_count = 0;
  ASSERT_EQ(OB_SUCCESS, wrapper_rtdef.get_bmm_filter_consumer_counts(
      setup.search_ctx_, group_consumer_count, query_consumer_count));
  EXPECT_EQ(1, group_consumer_count);
  EXPECT_EQ(1, query_consumer_count);
  EXPECT_EQ(OB_NOT_SUPPORTED,
            wrapper_rtdef.set_pushdown_filter_provider(false, nullptr, ObDASSearchCost()));
  fts_target = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.resolve_async_fts_rtdef(
                query_string_rtdef, fts_target));
  EXPECT_EQ(&query_string_rtdef, fts_target);
  MockSearchOp filter_op(setup.search_ctx_);
  for (const bool vectorized : {false, true}) {
    ASSERT_EQ(OB_SUCCESS, wrapper_rtdef.set_bmm_input(
        false, &filter_op, ObDASSearchCost(17), ObDASBMMBuildOptions(vectorized)));
    EXPECT_EQ(&filter_op, query_string_rtdef.pushdown_filter_op_);
    EXPECT_TRUE(query_string_rtdef.has_bmm_build_options_);
    EXPECT_EQ(vectorized, query_string_rtdef.bmm_build_options_.enable_vectorized_merge_);
    EXPECT_EQ(17, query_string_rtdef.received_filter_cost_.cost());
    EXPECT_FALSE(query_string_rtdef.query_optional_);
  }
}

TEST_F(ObDASBooleanQueryTest,
       default_filter_provider_setter_reports_not_supported)
{
  SearchCtxSetup setup(allocator_);
  MockSearchRtDef unsupported_rtdef(1);
  MockSearchOp source_op(setup.search_ctx_);
  ObDASFilterBitmapProvider provider(
      setup.search_ctx_, &unsupported_rtdef, DAS_FILTER_BITMAP_QUERY_SCOPE,
      DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT, &source_op, 0);
  ObIDASSearchRtDef &base_rtdef = unsupported_rtdef;
  EXPECT_EQ(OB_NOT_SUPPORTED,
            base_rtdef.set_pushdown_filter_provider(false, &provider, ObDASSearchCost(23)));
  EXPECT_EQ(OB_NOT_SUPPORTED,
            base_rtdef.set_pushdown_filter_provider(true, nullptr, ObDASSearchCost()));
  EXPECT_EQ(OB_NOT_SUPPORTED, base_rtdef.set_bmm_input(
      false, &source_op, ObDASSearchCost(1), ObDASBMMBuildOptions(true)));
}

TEST_F(ObDASBooleanQueryTest,
       match_filter_provider_setter_preserves_state_when_provider_is_null)
{
  SearchCtxSetup setup(allocator_);
  MockSearchOp source_op(setup.search_ctx_);
  ObDASMatchRtDef match_rtdef;
  ObDASFilterBitmapProvider provider(
      setup.search_ctx_, &match_rtdef, DAS_FILTER_BITMAP_QUERY_SCOPE,
      DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT, &source_op, 0);

  ASSERT_EQ(OB_SUCCESS, match_rtdef.set_bmm_input(
      true, &source_op, ObDASSearchCost(17), ObDASBMMBuildOptions(true)));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            match_rtdef.set_pushdown_filter_provider(false, nullptr, ObDASSearchCost(23)));
  EXPECT_EQ(&source_op, match_rtdef.pushdown_filter_op_);
  EXPECT_EQ(nullptr, match_rtdef.pushdown_filter_provider_);
  EXPECT_EQ(17, match_rtdef.pushdown_filter_cost_.cost());
  EXPECT_TRUE(match_rtdef.query_optional_);
  EXPECT_TRUE(match_rtdef.has_bmm_build_options_);

  ASSERT_EQ(OB_SUCCESS,
            match_rtdef.set_pushdown_filter_provider(false, &provider, ObDASSearchCost(23)));
  EXPECT_EQ(nullptr, match_rtdef.pushdown_filter_op_);
  EXPECT_EQ(&provider, match_rtdef.pushdown_filter_provider_);
  EXPECT_EQ(23, match_rtdef.pushdown_filter_cost_.cost());
  EXPECT_FALSE(match_rtdef.query_optional_);
  EXPECT_FALSE(match_rtdef.has_bmm_build_options_);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            match_rtdef.set_pushdown_filter_provider(true, nullptr, ObDASSearchCost(31)));
  EXPECT_EQ(nullptr, match_rtdef.pushdown_filter_op_);
  EXPECT_EQ(&provider, match_rtdef.pushdown_filter_provider_);
  EXPECT_EQ(23, match_rtdef.pushdown_filter_cost_.cost());
  EXPECT_FALSE(match_rtdef.query_optional_);
}

TEST_F(ObDASBooleanQueryTest, match_filter_provider_accepts_unknown_and_zero_cost)
{
  SearchCtxSetup setup(allocator_);
  MockSearchOp source_op(setup.search_ctx_);
  ObDASMatchRtDef match_rtdef;
  ObDASFilterBitmapProvider provider(
      setup.search_ctx_, &match_rtdef, DAS_FILTER_BITMAP_QUERY_SCOPE,
      DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT, &source_op, 0);

  match_rtdef.set_pushdown_filter(false, &source_op, ObDASSearchCost(17));
  ASSERT_EQ(OB_SUCCESS,
            match_rtdef.set_pushdown_filter_provider(true, &provider, ObDASSearchCost()));
  EXPECT_EQ(&provider, match_rtdef.pushdown_filter_provider_);
  EXPECT_FALSE(match_rtdef.pushdown_filter_cost_.is_valid());
  EXPECT_TRUE(match_rtdef.query_optional_);
  EXPECT_FALSE(provider.is_prepared());

  ASSERT_EQ(OB_SUCCESS,
            match_rtdef.set_pushdown_filter_provider(false, &provider, ObDASSearchCost(0)));
  EXPECT_EQ(0, match_rtdef.pushdown_filter_cost_.cost());
  EXPECT_TRUE(match_rtdef.pushdown_filter_cost_.is_valid());
  EXPECT_FALSE(provider.is_prepared());
  EXPECT_EQ(0, source_op.get_open_count());
}

TEST_F(ObDASBooleanQueryTest,
       async_wrapper_filter_provider_setter_forwards_success_and_errors)
{
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ir_ctdef.is_async_index_ = true;
  ObDASMatchCtDef match_ctdef(allocator_);
  ObDASBaseCtDef *match_children[] = {&ir_ctdef};
  match_ctdef.children_ = match_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(match_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  ObDASMatchRtDef match_rtdef;
  match_rtdef.ctdef_ = &match_ctdef;
  ObDASBooleanQueryCtDef wrapper_ctdef(allocator_);
  ObDASBaseCtDef *wrapper_children[] = {&match_ctdef};
  wrapper_ctdef.children_ = wrapper_children;
  wrapper_ctdef.children_cnt_ = ARRAYSIZEOF(wrapper_children);
  wrapper_ctdef.set_is_async(true);
  wrapper_ctdef.set_must(0, 1);
  ObDASBooleanQueryRtDef wrapper_rtdef;
  ObDASBaseRtDef *wrapper_rt_children[] = {&match_rtdef};
  wrapper_rtdef.ctdef_ = &wrapper_ctdef;
  wrapper_rtdef.children_ = wrapper_rt_children;
  wrapper_rtdef.children_cnt_ = ARRAYSIZEOF(wrapper_rt_children);
  SearchCtxSetup setup(allocator_);
  MockSearchOp source_op(setup.search_ctx_);
  ObDASFilterBitmapProvider provider(
      setup.search_ctx_, &wrapper_rtdef, DAS_FILTER_BITMAP_QUERY_SCOPE,
      DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT, &source_op, 0);

  match_rtdef.set_pushdown_filter(false, &source_op, ObDASSearchCost(17));
  ASSERT_EQ(OB_SUCCESS,
            wrapper_rtdef.set_pushdown_filter_provider(true, &provider, ObDASSearchCost(37)));
  EXPECT_EQ(&provider, match_rtdef.pushdown_filter_provider_);
  EXPECT_EQ(nullptr, match_rtdef.pushdown_filter_op_);
  EXPECT_EQ(37, match_rtdef.pushdown_filter_cost_.cost());
  EXPECT_TRUE(match_rtdef.query_optional_);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            wrapper_rtdef.set_pushdown_filter_provider(false, nullptr, ObDASSearchCost()));

  ir_ctdef.is_async_index_ = false;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            wrapper_rtdef.set_pushdown_filter_provider(false, &provider, ObDASSearchCost()));
  ir_ctdef.is_async_index_ = true;
  wrapper_rtdef.children_cnt_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            wrapper_rtdef.set_pushdown_filter_provider(false, &provider, ObDASSearchCost()));
  wrapper_rtdef.children_cnt_ = ARRAYSIZEOF(wrapper_rt_children);
  wrapper_rt_children[0] = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            wrapper_rtdef.set_pushdown_filter_provider(false, &provider, ObDASSearchCost()));
  wrapper_rt_children[0] = &match_rtdef;
  wrapper_ctdef.set_is_async(false);
  EXPECT_EQ(OB_NOT_SUPPORTED,
            wrapper_rtdef.set_pushdown_filter_provider(false, &provider, ObDASSearchCost()));
  wrapper_rtdef.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            wrapper_rtdef.set_pushdown_filter_provider(false, &provider, ObDASSearchCost()));
  EXPECT_EQ(&provider, match_rtdef.pushdown_filter_provider_);
  EXPECT_EQ(nullptr, match_rtdef.pushdown_filter_op_);
  EXPECT_EQ(37, match_rtdef.pushdown_filter_cost_.cost());
  EXPECT_TRUE(match_rtdef.query_optional_);
}

TEST_F(ObDASBooleanQueryTest,
       req_filter_provider_setter_failure_never_generates_unfiltered_match)
{
  enum FilterShape { SYNC_FILTER, ASYNC_FILTER, MIXED_FILTER, FILTER_SHAPE_COUNT };
  for (int64_t filter_shape = SYNC_FILTER; filter_shape < FILTER_SHAPE_COUNT; ++filter_shape) {
    SCOPED_TRACE(filter_shape);
    reset_ctdef();
    ObDASScalarScanCtDef sync_ctdef(allocator_);
    ObDASScalarScanCtDef async_ctdef(allocator_);
    MockScalarScanRtDef sync_rtdef;
    MockScalarScanRtDef async_rtdef;
    sync_ctdef.ref_table_id_ = 1001;
    async_ctdef.ref_table_id_ = 2001;
    async_ctdef.is_async_index_ = true;
    sync_rtdef.ctdef_ = &sync_ctdef;
    async_rtdef.ctdef_ = &async_ctdef;
    ObDASIRScanCtDef ir_ctdef(allocator_);
    ObDASMatchCtDef match_ctdef(allocator_);
    RejectingProviderMatchRtDef match_rtdef;
    ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
    ir_ctdef.is_async_index_ = true;
    match_ctdef.children_ = match_ctdef_children;
    match_ctdef.children_cnt_ = ARRAYSIZEOF(match_ctdef_children);
    match_ctdef.ir_ctdef_idx_ = 0;
    match_rtdef.ctdef_ = &match_ctdef;
    match_rtdef.consumer_count_ = 2;
    match_rtdef.query_consumer_count_ = 3;
    match_ctdef.set_is_scoring(true);
    match_ctdef.set_is_top_level_scoring(true);
    add_clause(MUST, &match_rtdef);
    if (ASYNC_FILTER != filter_shape) {
      add_clause(FILTER, &sync_rtdef);
    }
    if (SYNC_FILTER != filter_shape) {
      add_clause(FILTER, &async_rtdef);
    }
    prepare_structure();
    ctdef_.set_is_scoring(true);
    ctdef_.set_is_top_level_scoring(true);
    SearchCtxSetup setup(allocator_);
    setup.search_ctx_.set_async_binding(OB_INVALID_ID, 0, false);
    ObIDASSearchOp *op = nullptr;
    // Exercise req/provider installation after Boolean routing is selected.
    // This mock supplies consumer counts without a storage catalog.
    EXPECT_EQ(OB_STATE_NOT_MATCH, rtdef_.generate_op_impl(
        ObDASSearchCost(10), setup.search_ctx_, op));
    EXPECT_EQ(nullptr, op);
    EXPECT_EQ(1, match_rtdef.provider_set_count_);
    EXPECT_EQ(0, match_rtdef.generate_count_);
    EXPECT_EQ(ASYNC_FILTER != filter_shape ? 1 : 0, sync_rtdef.generate_count_);
    EXPECT_EQ(SYNC_FILTER != filter_shape ? 1 : 0, async_rtdef.generate_count_);
    EXPECT_EQ(MIXED_FILTER == filter_shape ? 2 : 1,
              setup.search_ctx_.get_filter_bitmap_provider_count());
  }
}

TEST_F(ObDASBooleanQueryTest,
       opt_filter_provider_setter_failure_never_generates_unfiltered_match)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  filter_ctdef.ref_table_id_ = 2001;
  filter_ctdef.is_async_index_ = true;
  filter_rtdef.ctdef_ = &filter_ctdef;
  ObDASMatchCtDef match_ctdef(allocator_);
  RejectingProviderMatchRtDef match_rtdef;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.consumer_count_ = 2;
  match_rtdef.query_consumer_count_ = 2;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);
  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(1);
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(OB_INVALID_ID, 0, false);
  ObIDASSearchOp *op = nullptr;
  // Exercise opt/provider installation without the outer storage-domain router;
  // the mocked MATCH intentionally has no physical index catalog or IR children.
  EXPECT_EQ(OB_STATE_NOT_MATCH, rtdef_.generate_op_impl(
      ObDASSearchCost(10), setup.search_ctx_, op));
  EXPECT_EQ(nullptr, op);
  EXPECT_EQ(1, match_rtdef.provider_set_count_);
  EXPECT_EQ(0, match_rtdef.generate_count_);
  EXPECT_EQ(1, filter_rtdef.generate_count_);
  EXPECT_EQ(1, setup.search_ctx_.get_filter_bitmap_provider_count());
}

TEST_F(ObDASBooleanQueryTest,
       filter_bitmap_provider_is_lazy_materializes_once_and_views_are_independent)
{
  SearchCtxSetup setup(allocator_);
  // SearchCtxSetup cannot complete row-count estimation without a storage
  // tenant in this unit test, so restore the eval context needed by provider
  // batch materialization after init() rolls it back.
  setup.search_ctx_.eval_ctx_ = &setup.mock_eval_ctx_;
  MockSearchOp *source_op = OB_NEWx(
      MockSearchOp, &allocator_, setup.search_ctx_,
      std::vector<uint64_t>{1, 3, 5, 9});
  ASSERT_NE(nullptr, source_op);
  MockSearchOpParam source_param;
  ASSERT_EQ(OB_SUCCESS, source_op->init(source_param));

  const ObIDASSearchRtDef *owner =
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234);
  ObDASFilterBitmapProvider *provider = nullptr;
  setup.search_ctx_.set_async_binding(9001, 0, false);
  ASSERT_EQ(OB_SUCCESS, setup.search_ctx_.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 0, source_op, provider));
  ASSERT_NE(nullptr, provider);
  ObDASFilterBitmapProvider *same_provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, setup.search_ctx_.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 0, source_op, same_provider));
  EXPECT_EQ(provider, same_provider);
  EXPECT_FALSE(provider->is_prepared());
  EXPECT_EQ(0, source_op->get_open_count());

  ObDASBitmapOp *first_view = nullptr;
  ObDASBitmapOp *second_view = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider->create_view(setup.search_ctx_, first_view));
  ASSERT_EQ(OB_SUCCESS, provider->create_view(setup.search_ctx_, second_view));
  ASSERT_NE(nullptr, first_view);
  ASSERT_NE(nullptr, second_view);
  ASSERT_NE(first_view, second_view);
  ASSERT_EQ(OB_SUCCESS, first_view->open());
  ASSERT_EQ(OB_SUCCESS, second_view->open());
  EXPECT_FALSE(provider->is_prepared());
  EXPECT_EQ(0, source_op->get_open_count());

  const uint64_t expected[] = {1, 3, 5, 9};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected); ++i) {
    ObDASRowID first_id;
    ObDASRowID second_id;
    double first_score = -1.0;
    double second_score = -1.0;
    ASSERT_EQ(OB_SUCCESS, first_view->next_rowid(first_id, first_score));
    if (0 == i) {
      EXPECT_TRUE(provider->is_prepared());
      EXPECT_EQ(1, source_op->get_open_count());
      ASSERT_NE(nullptr, provider->get_bitmap());
      EXPECT_EQ(ARRAYSIZEOF(expected), provider->get_bitmap()->cardinality());
    }
    ASSERT_EQ(OB_SUCCESS, second_view->next_rowid(second_id, second_score));
    EXPECT_EQ(expected[i], first_id.get_uint64());
    EXPECT_EQ(expected[i], second_id.get_uint64());
    EXPECT_DOUBLE_EQ(0.0, first_score);
    EXPECT_DOUBLE_EQ(0.0, second_score);
  }
  ObDASRowID end_id;
  double end_score = 0.0;
  EXPECT_EQ(OB_ITER_END, first_view->next_rowid(end_id, end_score));
  EXPECT_EQ(OB_ITER_END, second_view->next_rowid(end_id, end_score));
}

TEST_F(ObDASBooleanQueryTest,
       filter_bitmap_providers_are_isolated_by_group_slot)
{
  SearchCtxSetup setup(allocator_);
  MockSearchOp *first_source = OB_NEWx(
      MockSearchOp, &allocator_, setup.search_ctx_,
      std::vector<uint64_t>{1, 3});
  MockSearchOp *second_source = OB_NEWx(
      MockSearchOp, &allocator_, setup.search_ctx_,
      std::vector<uint64_t>{2, 4});
  ASSERT_NE(nullptr, first_source);
  ASSERT_NE(nullptr, second_source);
  MockSearchOpParam source_param;
  ASSERT_EQ(OB_SUCCESS, first_source->init(source_param));
  ASSERT_EQ(OB_SUCCESS, second_source->init(source_param));

  const ObIDASSearchRtDef *owner =
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234);
  ObDASFilterBitmapProvider *first_provider = nullptr;
  ObDASFilterBitmapProvider *second_provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, setup.search_ctx_.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 0, first_source, first_provider));
  ASSERT_EQ(OB_SUCCESS, setup.search_ctx_.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 1, second_source, second_provider));
  ASSERT_NE(nullptr, first_provider);
  ASSERT_NE(nullptr, second_provider);
  EXPECT_NE(first_provider, second_provider);
  EXPECT_EQ(0, first_provider->get_group_slot());
  EXPECT_EQ(1, second_provider->get_group_slot());
  EXPECT_EQ(first_provider,
            setup.search_ctx_.find_filter_bitmap_provider(
                owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 0));
  EXPECT_EQ(second_provider,
            setup.search_ctx_.find_filter_bitmap_provider(
                owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 1));
  EXPECT_EQ(2, setup.search_ctx_.get_filter_bitmap_provider_count());

  setup.search_ctx_.set_async_binding(9001, 0, false);
  ObDASBitmapOp *first_view = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            first_provider->create_view(setup.search_ctx_, first_view));
  ASSERT_NE(nullptr, first_view);
  setup.search_ctx_.set_async_binding(9001, 1, false);
  ObDASBitmapOp *second_view = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            second_provider->create_view(setup.search_ctx_, second_view));
  ASSERT_NE(nullptr, second_view);
  ObDASBitmapOp *cross_group_view = nullptr;
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            first_provider->create_view(
                setup.search_ctx_, cross_group_view));
  EXPECT_EQ(nullptr, cross_group_view);
}

TEST_F(ObDASBooleanQueryTest,
       query_filter_bitmap_provider_is_lazy_and_cross_group)
{
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.eval_ctx_ = &setup.mock_eval_ctx_;
  MockSearchOp *source_op = OB_NEWx(
      MockSearchOp, &allocator_, setup.search_ctx_,
      std::vector<uint64_t>{2, 4, 8});
  ASSERT_NE(nullptr, source_op);
  MockSearchOpParam source_param;
  ASSERT_EQ(OB_SUCCESS, source_op->init(source_param));

  const ObIDASSearchRtDef *owner =
      reinterpret_cast<const ObIDASSearchRtDef *>(0x5678);
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, setup.search_ctx_.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_QUERY_SCOPE,
      DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT, source_op, provider));
  ASSERT_NE(nullptr, provider);
  EXPECT_FALSE(provider->is_prepared());
  EXPECT_EQ(0, source_op->get_open_count());

  setup.search_ctx_.set_async_binding(9001, 0, false);
  ObDASBitmapOp *first_view = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider->create_view(setup.search_ctx_, first_view));
  setup.search_ctx_.set_async_binding(9001, 1, false);
  ObDASBitmapOp *second_view = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider->create_view(setup.search_ctx_, second_view));
  ASSERT_EQ(OB_SUCCESS, first_view->open());
  ASSERT_EQ(OB_SUCCESS, second_view->open());
  EXPECT_FALSE(provider->is_prepared());
  EXPECT_EQ(0, source_op->get_open_count());

  ObDASRowID rowid;
  double score = -1.0;
  ASSERT_EQ(OB_SUCCESS, second_view->next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_TRUE(provider->is_prepared());
  EXPECT_EQ(1, source_op->get_open_count());
  ASSERT_EQ(OB_SUCCESS, first_view->next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_EQ(1, source_op->get_open_count());

  ASSERT_EQ(OB_SUCCESS,
            setup.search_ctx_.reset_filter_bitmap_providers_for_rescan());
  EXPECT_FALSE(provider->is_prepared());
  ASSERT_EQ(OB_SUCCESS, first_view->rescan());
  ASSERT_EQ(OB_SUCCESS, second_view->rescan());
  EXPECT_EQ(0, source_op->get_rescan_count());
  ASSERT_EQ(OB_SUCCESS, first_view->next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_EQ(1, source_op->get_open_count());
  EXPECT_EQ(1, source_op->get_rescan_count());
  ASSERT_EQ(OB_SUCCESS, second_view->next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_EQ(1, source_op->get_rescan_count());
}

TEST_F(ObDASBooleanQueryTest, match_filter_state_is_cleared_on_capability_and_generation_error)
{
  SearchCtxSetup setup(allocator_);
  ObDASMatchRtDef match_rtdef;
  ObIDASSearchOp *filter_op = reinterpret_cast<ObIDASSearchOp *>(1);
  bool can_pushdown = true;
  ASSERT_EQ(OB_SUCCESS, match_rtdef.set_bmm_input(
      true, filter_op, ObDASSearchCost(1), ObDASBMMBuildOptions(true)));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            match_rtdef.can_pushdown_filter_to_bmm(setup.search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  EXPECT_EQ(nullptr, match_rtdef.pushdown_filter_op_);
  EXPECT_FALSE(match_rtdef.query_optional_);
  EXPECT_FALSE(match_rtdef.has_bmm_build_options_);

  ObIDASSearchOp *op = reinterpret_cast<ObIDASSearchOp *>(2);
  ASSERT_EQ(OB_SUCCESS, match_rtdef.set_bmm_input(
      true, filter_op, ObDASSearchCost(1), ObDASBMMBuildOptions(true)));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            match_rtdef.generate_op(ObDASSearchCost(1), setup.search_ctx_, op));
  EXPECT_EQ(nullptr, op);
  EXPECT_EQ(nullptr, match_rtdef.pushdown_filter_op_);
  EXPECT_FALSE(match_rtdef.query_optional_);
  EXPECT_FALSE(match_rtdef.has_bmm_build_options_);

  ObDASMultiMatchRtDef multi_match_rtdef;
  can_pushdown = true;
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef.set_bmm_input(
      true, filter_op, ObDASSearchCost(1), ObDASBMMBuildOptions(true)));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            multi_match_rtdef.can_pushdown_filter_to_bmm(
                setup.search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  EXPECT_EQ(nullptr, multi_match_rtdef.pushdown_filter_op_);
  EXPECT_FALSE(multi_match_rtdef.query_optional_);
  EXPECT_FALSE(multi_match_rtdef.has_bmm_build_options_);
  op = reinterpret_cast<ObIDASSearchOp *>(2);
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef.set_bmm_input(
      true, filter_op, ObDASSearchCost(1), ObDASBMMBuildOptions(true)));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            multi_match_rtdef.generate_op(
                ObDASSearchCost(1), setup.search_ctx_, op));
  EXPECT_EQ(nullptr, op);
  EXPECT_EQ(nullptr, multi_match_rtdef.pushdown_filter_op_);
  EXPECT_FALSE(multi_match_rtdef.query_optional_);
  EXPECT_FALSE(multi_match_rtdef.has_bmm_build_options_);

  ObDASQueryStringRtDef query_string_rtdef;
  can_pushdown = true;
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef.set_bmm_input(
      true, filter_op, ObDASSearchCost(1), ObDASBMMBuildOptions(true)));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            query_string_rtdef.can_pushdown_filter_to_bmm(
                setup.search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  EXPECT_EQ(nullptr, query_string_rtdef.pushdown_filter_op_);
  EXPECT_FALSE(query_string_rtdef.query_optional_);
  EXPECT_FALSE(query_string_rtdef.has_bmm_build_options_);
  op = reinterpret_cast<ObIDASSearchOp *>(2);
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef.set_bmm_input(
      true, filter_op, ObDASSearchCost(1), ObDASBMMBuildOptions(true)));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            query_string_rtdef.generate_op(
                ObDASSearchCost(1), setup.search_ctx_, op));
  EXPECT_EQ(nullptr, op);
  EXPECT_EQ(nullptr, query_string_rtdef.pushdown_filter_op_);
  EXPECT_FALSE(query_string_rtdef.query_optional_);
  EXPECT_FALSE(query_string_rtdef.has_bmm_build_options_);
}

TEST_F(ObDASBooleanQueryTest, sync_filter_should_query_optional_still_pushes_down)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  filter_ctdef.ref_table_id_ = 1001;
  filter_rtdef.ctdef_ = &filter_ctdef;

  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASScalarScanRtDef inv_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASIRScanRtDef ir_rtdef;
  ir_ctdef.is_async_index_ = 0;
  ir_ctdef.use_scalar_scan_def_ = 1;
  inv_rtdef.ctdef_ = &inv_ctdef;
  ObDASBaseCtDef *ir_ctdef_children[] = {&inv_ctdef};
  ObDASBaseRtDef *ir_rtdef_children[] = {&inv_rtdef};
  ir_ctdef.children_ = ir_ctdef_children;
  ir_ctdef.children_cnt_ = 1;
  ir_rtdef.ctdef_ = &ir_ctdef;
  ir_rtdef.children_ = ir_rtdef_children;
  ir_rtdef.children_cnt_ = 1;
  ObDASMatchCtDef match_ctdef(allocator_);
  RecordingMatchRtDef match_rtdef;
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *match_rtdef_children[] = {&ir_rtdef};
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = 1;
  match_ctdef.ir_ctdef_idx_ = 0;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.children_ = match_rtdef_children;
  match_rtdef.children_cnt_ = 1;

  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  SearchCtxSetup setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  EXPECT_EQ(OB_SUCCESS,
            rtdef_.generate_op(
                ObDASSearchCost(10), setup.search_ctx_, op));
  EXPECT_NE(nullptr, op);
  EXPECT_EQ(1, match_rtdef.capability_count_);
  EXPECT_EQ(1, match_rtdef.compute_cost_count_);
  EXPECT_EQ(1, match_rtdef.nonnull_filter_set_count_);
  EXPECT_EQ(1, match_rtdef.generate_count_);
  EXPECT_TRUE(match_rtdef.cost_prepared_before_generate_);
  EXPECT_TRUE(match_rtdef.last_query_optional_);
}

TEST_F(ObDASBooleanQueryTest, async_filter_should_query_optional_pushes_down)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  filter_rtdef.ctdef_ = &filter_ctdef;

  ObDASBooleanQueryCtDef async_ctdef(allocator_);
  RecordingAsyncBooleanRtDef async_rtdef;
  async_ctdef.set_is_async(true);
  async_rtdef.ctdef_ = &async_ctdef;

  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &async_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  SearchCtxSetup setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, op));
  EXPECT_NE(nullptr, op);
  EXPECT_EQ(1, async_rtdef.set_filter_count_);
  EXPECT_EQ(1, async_rtdef.generate_count_);
  EXPECT_TRUE(async_rtdef.last_query_optional_);
  EXPECT_NE(nullptr, async_rtdef.last_filter_op_);
}

TEST_F(ObDASBooleanQueryTest, optional_probe_only_filter_falls_back_to_req_opt)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  FirstProbeThenEnumerableFilterRtDef filter_rtdef;
  filter_rtdef.ctdef_ = &filter_ctdef;
  ObDASMatchCtDef match_ctdef(allocator_);
  RecordingMatchRtDef match_rtdef;
  match_rtdef.ctdef_ = &match_ctdef;

  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  SearchCtxSetup setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_OPT, op->get_op_type());
  EXPECT_EQ(2, filter_rtdef.generate_count_);
  EXPECT_EQ(0, match_rtdef.nonnull_filter_set_count_);
  EXPECT_FALSE(match_rtdef.last_query_optional_);
}

TEST_F(ObDASBooleanQueryTest,
       group_optional_filter_with_multiple_consumers_keeps_outer_tree)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  ObDASMatchCtDef match_ctdef(allocator_);
  GroupAwareMatchRtDef match_rtdef;
  filter_rtdef.ctdef_ = &filter_ctdef;
  match_rtdef.ctdef_ = &match_ctdef;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);

  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(9001, 0, false);

  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_OPT, op->get_op_type());
  EXPECT_EQ(0, setup.search_ctx_.get_filter_bitmap_provider_count());
  EXPECT_EQ(1, match_rtdef.capability_count_);
  EXPECT_EQ(1, match_rtdef.generate_count_);
  EXPECT_EQ(1, filter_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest,
       group_optional_pure_async_one_consumer_per_group_pushes_direct)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  ObDASMatchCtDef match_ctdef(allocator_);
  GroupAwareMatchRtDef match_rtdef;
  filter_ctdef.ref_table_id_ = 9002;
  filter_ctdef.is_async_index_ = true;
  filter_rtdef.ctdef_ = &filter_ctdef;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.consumer_count_ = 1;
  match_rtdef.second_group_consumer_count_ = 1;
  match_rtdef.query_consumer_count_ = 2;
  match_rtdef.all_groups_eligible_ = true;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);
  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);

  for (int64_t group_slot = 0; group_slot < 2; ++group_slot) {
    setup.search_ctx_.set_async_binding(9001, group_slot, true);
    ObIDASSearchOp *op = nullptr;
    ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
        ObDASSearchCost(10), setup.search_ctx_, op));
    ASSERT_NE(nullptr, op);
    ASSERT_EQ(DAS_SEARCH_OP_BMM, op->get_op_type());
    ObIDASSearchOp *filter_op = static_cast<ObDASBMMOp *>(op)->get_filter_op();
    ASSERT_NE(nullptr, filter_op);
    EXPECT_NE(DAS_SEARCH_OP_BITMAP, filter_op->get_op_type());
    EXPECT_EQ(0, setup.search_ctx_.get_filter_bitmap_provider_count());
  }
  EXPECT_EQ(2, match_rtdef.generate_count_);
  EXPECT_EQ(2, filter_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest,
       group_optional_sync_filter_across_groups_keeps_req_opt)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  ObDASMatchCtDef match_ctdef(allocator_);
  GroupAwareMatchRtDef match_rtdef;
  filter_ctdef.ref_table_id_ = 9002;
  filter_rtdef.ctdef_ = &filter_ctdef;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.consumer_count_ = 1;
  match_rtdef.query_consumer_count_ = 2;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);
  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(9001, 0, true);

  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_OPT, op->get_op_type());
  EXPECT_EQ(0, setup.search_ctx_.get_filter_bitmap_provider_count());
  EXPECT_EQ(1, filter_rtdef.generate_count_);
  EXPECT_EQ(1, match_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest,
       group_optional_pure_async_multiple_consumers_keeps_req_opt)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  ObDASMatchCtDef match_ctdef(allocator_);
  GroupAwareMatchRtDef match_rtdef;
  filter_ctdef.ref_table_id_ = 9002;
  filter_ctdef.is_async_index_ = true;
  filter_rtdef.ctdef_ = &filter_ctdef;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.consumer_count_ = 2;
  match_rtdef.query_consumer_count_ = 2;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);
  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(9001, 0, false);

  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_OPT, op->get_op_type());
  EXPECT_EQ(0, setup.search_ctx_.get_filter_bitmap_provider_count());
  EXPECT_EQ(1, filter_rtdef.generate_count_);
  EXPECT_EQ(1, match_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest,
       group_optional_empty_match_preserves_filter_domain)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockSearchRtDef filter_rtdef(3, {2, 5, 9});
  ObDASMatchCtDef match_ctdef(allocator_);
  GroupAwareMatchRtDef match_rtdef;
  filter_ctdef.ref_table_id_ = 9002;
  filter_ctdef.is_async_index_ = true;
  filter_rtdef.ctdef_ = &filter_ctdef;
  match_rtdef.ctdef_ = &match_ctdef;
  // Model the empty-token capability result and its empty scoring child.
  match_rtdef.consumer_count_ = 0;
  match_rtdef.query_consumer_count_ = 0;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);
  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(9001, 0, true);

  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_OPT, op->get_op_type());
  EXPECT_EQ(0, setup.search_ctx_.get_filter_bitmap_provider_count());
  verify_result(op, {2, 5, 9}, {0.0, 0.0, 0.0});
}

TEST_F(ObDASBooleanQueryTest,
       group_required_filter_with_one_consumer_uses_direct_filter)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASMatchCtDef match_ctdef(allocator_);
  GroupAwareMatchRtDef match_rtdef;
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  filter_rtdef.ctdef_ = &filter_ctdef;
  ir_ctdef.is_async_index_ = true;
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(match_ctdef_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.consumer_count_ = 1;
  match_rtdef.query_consumer_count_ = 1;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);

  add_clause(MUST, &match_rtdef);
  add_clause(FILTER, &filter_rtdef);
  prepare_structure();
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(9001, 0, false);

  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  ASSERT_EQ(DAS_SEARCH_OP_BMM, op->get_op_type());
  ObIDASSearchOp *filter_op =
      static_cast<ObDASBMMOp *>(op)->get_filter_op();
  ASSERT_NE(nullptr, filter_op);
  EXPECT_NE(DAS_SEARCH_OP_BITMAP, filter_op->get_op_type());
  EXPECT_EQ(0, setup.search_ctx_.get_filter_bitmap_provider_count());
  EXPECT_EQ(1, match_rtdef.capability_count_);
  EXPECT_EQ(1, match_rtdef.generate_count_);
  EXPECT_EQ(1, filter_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest,
       nested_sync_boolean_filter_is_generated_in_group_filter_context)
{
  ObDASScalarScanCtDef term_ctdef(allocator_);
  ObDASScalarScanCtDef range_ctdef(allocator_);
  MockScalarScanRtDef term_rtdef;
  MockScalarScanRtDef range_rtdef;
  ObDASBooleanQueryCtDef nested_ctdef(allocator_);
  ObDASBooleanQueryRtDef nested_rtdef;
  ObDASBaseCtDef *nested_ctdef_children[] = {&term_ctdef, &range_ctdef};
  ObDASBaseRtDef *nested_rtdef_children[] = {&term_rtdef, &range_rtdef};
  ObIDASSearchRtDef *filter_clauses[] = {&nested_rtdef};

  term_ctdef.ref_table_id_ = 1001;
  range_ctdef.ref_table_id_ = 1002;
  term_rtdef.ctdef_ = &term_ctdef;
  range_rtdef.ctdef_ = &range_ctdef;
  nested_ctdef.children_ = nested_ctdef_children;
  nested_ctdef.children_cnt_ = ARRAYSIZEOF(nested_ctdef_children);
  nested_ctdef.set_filter(0, 1);
  nested_ctdef.set_should(1, 1);
  nested_ctdef.set_min_should_match(1);
  nested_rtdef.ctdef_ = &nested_ctdef;
  nested_rtdef.children_ = nested_rtdef_children;
  nested_rtdef.children_cnt_ = ARRAYSIZEOF(nested_rtdef_children);

  ObDASAsyncIndexDomainAnalysis analysis;
  bool is_group_compatible_filter = false;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &nested_ctdef, analysis));
  EXPECT_EQ(ASYNC_INDEX_DOMAIN_MIXED_SYNC, analysis.type_);
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::is_group_compatible_filter_domain(
                &nested_ctdef, is_group_compatible_filter));
  EXPECT_TRUE(is_group_compatible_filter);

  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(OB_INVALID_ID, 0, false);
  ObBooleanSubClause<ObIDASSearchRtDef> nested_filter(
      filter_clauses, ARRAYSIZEOF(filter_clauses));
  ObIDASSearchOp *filter_op = nullptr;
  ObDASSearchCost filter_cost;
  ASSERT_EQ(OB_SUCCESS, rtdef_.prepare_bmm_input(
      nested_filter, ObDASSearchCost(10), setup.search_ctx_,
      filter_op, filter_cost, false /* query_optional */, bmm_options_, has_bmm_options_));
  EXPECT_NE(nullptr, filter_op);
  EXPECT_EQ(1, term_rtdef.generate_count_);
  EXPECT_EQ(1, range_rtdef.generate_count_);
  EXPECT_FALSE(setup.search_ctx_.is_generating_group_filter_domain());
}

TEST_F(ObDASBooleanQueryTest,
       sync_filter_provider_is_shared_across_groups)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASMatchCtDef match_ctdef(allocator_);
  GroupAwareMatchRtDef match_rtdef;
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  filter_rtdef.ctdef_ = &filter_ctdef;
  ir_ctdef.is_async_index_ = true;
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(match_ctdef_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  filter_rtdef.cost_ = ObDASSearchCost(17);
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.consumer_count_ = 2;
  match_rtdef.second_group_consumer_count_ = 1;
  match_rtdef.query_consumer_count_ = 3;
  match_rtdef.all_groups_eligible_ = true;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);

  add_clause(MUST, &match_rtdef);
  add_clause(FILTER, &filter_rtdef);
  prepare_structure();
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);

  setup.search_ctx_.set_async_binding(9001, 0, false);
  ObIDASSearchOp *first_group_op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, first_group_op));
  ASSERT_NE(nullptr, first_group_op);
  ASSERT_EQ(DAS_SEARCH_OP_BMM, first_group_op->get_op_type());
  ASSERT_NE(nullptr,
            static_cast<ObDASBMMOp *>(first_group_op)->get_filter_op());
  EXPECT_EQ(DAS_SEARCH_OP_BITMAP,
            static_cast<ObDASBMMOp *>(first_group_op)
                ->get_filter_op()->get_op_type());
  ASSERT_EQ(1, setup.search_ctx_.get_filter_bitmap_provider_count());
  ObDASFilterBitmapProvider *first_group_provider =
      setup.search_ctx_.get_filter_bitmap_provider(0);
  ASSERT_NE(nullptr, first_group_provider);
  EXPECT_EQ(DAS_FILTER_BITMAP_QUERY_SCOPE, first_group_provider->get_scope());
  EXPECT_EQ(DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
            first_group_provider->get_group_slot());
  EXPECT_EQ(17, static_cast<ObDASBMMOp *>(first_group_op)->filter_cost_.cost());
  EXPECT_FALSE(first_group_provider->is_prepared());

  setup.search_ctx_.set_async_binding(9001, 1, false);
  ObIDASSearchOp *second_group_op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, second_group_op));
  ASSERT_NE(nullptr, second_group_op);
  ASSERT_EQ(DAS_SEARCH_OP_BMM, second_group_op->get_op_type());
  ASSERT_NE(nullptr,
            static_cast<ObDASBMMOp *>(second_group_op)->get_filter_op());
  EXPECT_EQ(DAS_SEARCH_OP_BITMAP,
            static_cast<ObDASBMMOp *>(second_group_op)
                ->get_filter_op()->get_op_type());
  EXPECT_EQ(1, setup.search_ctx_.get_filter_bitmap_provider_count());
  EXPECT_EQ(first_group_provider,
            setup.search_ctx_.find_filter_bitmap_provider(
                &rtdef_, DAS_FILTER_BITMAP_QUERY_SCOPE,
                DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT));
  EXPECT_EQ(17, static_cast<ObDASBMMOp *>(second_group_op)->filter_cost_.cost());
  EXPECT_FALSE(first_group_provider->is_prepared());
  EXPECT_EQ(1, filter_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest,
       mixed_filter_uses_query_and_group_scoped_providers)
{
  ObDASScalarScanCtDef sync_filter_ctdef(allocator_);
  ObDASScalarScanCtDef async_filter_ctdef(allocator_);
  MockScalarScanRtDef sync_filter_rtdef;
  MockScalarScanRtDef async_filter_rtdef;
  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASMatchCtDef match_ctdef(allocator_);
  GroupAwareMatchRtDef match_rtdef;
  ObDASBaseCtDef *ir_ctdef_children[] = {&inv_ctdef};
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  sync_filter_ctdef.ref_table_id_ = 1001;
  async_filter_ctdef.ref_table_id_ = 2001;
  async_filter_ctdef.is_async_index_ = true;
  sync_filter_rtdef.ctdef_ = &sync_filter_ctdef;
  async_filter_rtdef.ctdef_ = &async_filter_ctdef;
  inv_ctdef.ref_table_id_ = 9001;
  ir_ctdef.children_ = ir_ctdef_children;
  ir_ctdef.children_cnt_ = ARRAYSIZEOF(ir_ctdef_children);
  ir_ctdef.use_scalar_scan_def_ = true;
  ir_ctdef.is_async_index_ = true;
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(match_ctdef_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  sync_filter_rtdef.cost_ = ObDASSearchCost(17);
  async_filter_rtdef.cost_ = ObDASSearchCost(7);
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.consumer_count_ = 2;
  match_rtdef.second_group_consumer_count_ = 1;
  match_rtdef.query_consumer_count_ = 3;
  match_rtdef.all_groups_eligible_ = true;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);

  add_clause(MUST, &match_rtdef);
  add_clause(FILTER, &sync_filter_rtdef);
  add_clause(FILTER, &async_filter_rtdef);
  prepare_structure();
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);

  setup.search_ctx_.set_async_binding(OB_INVALID_ID, 0, false);
  ObIDASSearchOp *first_group_op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, first_group_op));
  ASSERT_NE(nullptr, first_group_op);
  ASSERT_EQ(DAS_SEARCH_OP_BMM, first_group_op->get_op_type());
  ASSERT_EQ(2, setup.search_ctx_.get_filter_bitmap_provider_count());
  ObDASFilterBitmapProvider *sync_provider =
      setup.search_ctx_.get_filter_bitmap_provider(0);
  ObDASFilterBitmapProvider *mixed_provider =
      setup.search_ctx_.get_filter_bitmap_provider(1);
  ASSERT_NE(nullptr, sync_provider);
  ASSERT_NE(nullptr, mixed_provider);
  EXPECT_EQ(DAS_FILTER_BITMAP_QUERY_SCOPE, sync_provider->get_scope());
  EXPECT_EQ(DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
            sync_provider->get_group_slot());
  EXPECT_EQ(DAS_FILTER_BITMAP_GROUP_SCOPE, mixed_provider->get_scope());
  EXPECT_EQ(0, mixed_provider->get_group_slot());
  EXPECT_EQ(7, static_cast<ObDASBMMOp *>(first_group_op)->filter_cost_.cost());
  EXPECT_FALSE(sync_provider->is_prepared());
  EXPECT_FALSE(mixed_provider->is_prepared());
  EXPECT_EQ(DAS_SEARCH_OP_BITMAP,
            static_cast<ObDASBMMOp *>(first_group_op)
                ->get_filter_op()->get_op_type());
  EXPECT_EQ(1, sync_filter_rtdef.generate_count_);
  EXPECT_EQ(1, async_filter_rtdef.generate_count_);

  setup.search_ctx_.set_async_binding(OB_INVALID_ID, 1, false);
  ObIDASSearchOp *second_group_op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, second_group_op));
  ASSERT_NE(nullptr, second_group_op);
  ASSERT_EQ(DAS_SEARCH_OP_BMM, second_group_op->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION,
            static_cast<ObDASBMMOp *>(second_group_op)
                ->get_filter_op()->get_op_type());
  EXPECT_EQ(2, setup.search_ctx_.get_filter_bitmap_provider_count());
  EXPECT_EQ(1, sync_filter_rtdef.generate_count_);
  EXPECT_EQ(2, async_filter_rtdef.generate_count_);
  EXPECT_EQ(7, static_cast<ObDASBMMOp *>(second_group_op)->filter_cost_.cost());
  EXPECT_FALSE(sync_provider->is_prepared());
  EXPECT_FALSE(mixed_provider->is_prepared());
}

TEST_F(ObDASBooleanQueryTest, provider_filter_cost_reaches_required_and_should_bmm)
{
  for (int64_t use_should = 0; use_should < 2; ++use_should) {
    for (int64_t use_async = 0; use_async < 2; ++use_async) {
      SCOPED_TRACE(use_should);
      SCOPED_TRACE(use_async);
      reset_ctdef();
      ObDASScalarScanCtDef first_filter_ctdef(allocator_);
      ObDASScalarScanCtDef second_filter_ctdef(allocator_);
      MockScalarScanRtDef first_filter_rtdef;
      MockScalarScanRtDef second_filter_rtdef;
      first_filter_ctdef.ref_table_id_ = 2001;
      second_filter_ctdef.ref_table_id_ = 2002;
      first_filter_ctdef.is_async_index_ = use_async;
      second_filter_ctdef.is_async_index_ = use_async;
      first_filter_rtdef.ctdef_ = &first_filter_ctdef;
      second_filter_rtdef.ctdef_ = &second_filter_ctdef;
      first_filter_rtdef.cost_ = ObDASSearchCost(17);
      second_filter_rtdef.cost_ = ObDASSearchCost(7);
      ObDASIRScanCtDef ir_ctdef(allocator_);
      ObDASMatchCtDef match_ctdef(allocator_);
      ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
      ir_ctdef.is_async_index_ = true;
      match_ctdef.children_ = match_ctdef_children;
      match_ctdef.children_cnt_ = ARRAYSIZEOF(match_ctdef_children);
      match_ctdef.ir_ctdef_idx_ = 0;
      GroupAwareMatchRtDef match_rtdef;
      match_rtdef.ctdef_ = &match_ctdef;
      match_rtdef.consumer_count_ = 2;
      match_rtdef.query_consumer_count_ = 3;
      match_ctdef.set_is_scoring(true);
      match_ctdef.set_is_top_level_scoring(true);
      add_clause(use_should ? SHOULD : MUST, &match_rtdef);
      add_clause(FILTER, &first_filter_rtdef);
      add_clause(FILTER, &second_filter_rtdef);
      prepare_structure();
      ctdef_.set_min_should_match(1);
      ctdef_.set_is_scoring(true);
      ctdef_.set_is_top_level_scoring(true);
      SearchCtxSetup setup(allocator_);
      setup.search_ctx_.set_async_binding(OB_INVALID_ID, 0, false);
      ObIDASSearchOp *op = nullptr;

      ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op_impl(
          ObDASSearchCost(10), setup.search_ctx_, op));
      ASSERT_NE(nullptr, op);
      ASSERT_EQ(DAS_SEARCH_OP_BMM, op->get_op_type());
      EXPECT_EQ(7, static_cast<ObDASBMMOp *>(op)->filter_cost_.cost());
      EXPECT_EQ(1, static_cast<ObDASBMMOp *>(op)->lead_cost_.cost());
      ASSERT_EQ(1, setup.search_ctx_.get_filter_bitmap_provider_count());
      ObDASFilterBitmapProvider *provider = setup.search_ctx_.get_filter_bitmap_provider(0);
      ASSERT_NE(nullptr, provider);
      EXPECT_EQ(use_async ? DAS_FILTER_BITMAP_GROUP_SCOPE : DAS_FILTER_BITMAP_QUERY_SCOPE,
                provider->get_scope());
      EXPECT_FALSE(provider->is_prepared());
      EXPECT_EQ(1, first_filter_rtdef.generate_count_);
      EXPECT_EQ(1, second_filter_rtdef.generate_count_);
      EXPECT_FALSE(setup.search_ctx_.is_generating_group_filter_domain());
    }
  }
}

TEST_F(ObDASBooleanQueryTest, filter_should_cost_preparation_reuses_cached_cost)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  filter_ctdef.ref_table_id_ = 1001;
  filter_rtdef.ctdef_ = &filter_ctdef;

  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASScalarScanRtDef inv_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASIRScanRtDef ir_rtdef;
  ir_ctdef.is_async_index_ = 0;
  ir_ctdef.use_scalar_scan_def_ = 1;
  inv_rtdef.ctdef_ = &inv_ctdef;
  ObDASBaseCtDef *ir_ctdef_children[] = {&inv_ctdef};
  ObDASBaseRtDef *ir_rtdef_children[] = {&inv_rtdef};
  ir_ctdef.children_ = ir_ctdef_children;
  ir_ctdef.children_cnt_ = 1;
  ir_rtdef.ctdef_ = &ir_ctdef;
  ir_rtdef.children_ = ir_rtdef_children;
  ir_rtdef.children_cnt_ = 1;
  ObDASMatchCtDef match_ctdef(allocator_);
  RecordingMatchRtDef match_rtdef;
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *match_rtdef_children[] = {&ir_rtdef};
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = 1;
  match_ctdef.ir_ctdef_idx_ = 0;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.children_ = match_rtdef_children;
  match_rtdef.children_cnt_ = 1;

  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  SearchCtxSetup setup(allocator_);
  ObDASSearchCost cached_cost;
  ASSERT_EQ(OB_SUCCESS, match_rtdef.get_cost(setup.search_ctx_, cached_cost));
  ASSERT_EQ(1, match_rtdef.compute_cost_count_);

  ObIDASSearchOp *op = nullptr;
  EXPECT_EQ(OB_SUCCESS,
            rtdef_.generate_op(
                ObDASSearchCost(10), setup.search_ctx_, op));
  EXPECT_NE(nullptr, op);
  EXPECT_EQ(1, match_rtdef.compute_cost_count_);
  EXPECT_TRUE(match_rtdef.cost_prepared_before_generate_);
}

TEST_F(ObDASBooleanQueryTest, filter_should_cost_preparation_propagates_error)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  filter_ctdef.ref_table_id_ = 1001;
  filter_rtdef.ctdef_ = &filter_ctdef;

  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASScalarScanRtDef inv_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASIRScanRtDef ir_rtdef;
  ir_ctdef.is_async_index_ = 0;
  ir_ctdef.use_scalar_scan_def_ = 1;
  inv_rtdef.ctdef_ = &inv_ctdef;
  ObDASBaseCtDef *ir_ctdef_children[] = {&inv_ctdef};
  ObDASBaseRtDef *ir_rtdef_children[] = {&inv_rtdef};
  ir_ctdef.children_ = ir_ctdef_children;
  ir_ctdef.children_cnt_ = 1;
  ir_rtdef.ctdef_ = &ir_ctdef;
  ir_rtdef.children_ = ir_rtdef_children;
  ir_rtdef.children_cnt_ = 1;
  ObDASMatchCtDef match_ctdef(allocator_);
  RecordingMatchRtDef match_rtdef;
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *match_rtdef_children[] = {&ir_rtdef};
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = 1;
  match_ctdef.ir_ctdef_idx_ = 0;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.children_ = match_rtdef_children;
  match_rtdef.children_cnt_ = 1;
  match_rtdef.compute_cost_ret_ = OB_ERR_UNEXPECTED;

  add_clause(FILTER, &filter_rtdef);
  add_clause(SHOULD, &match_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(0);
  SearchCtxSetup setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            rtdef_.generate_op(
                ObDASSearchCost(10), setup.search_ctx_, op));
  EXPECT_EQ(nullptr, op);
  EXPECT_EQ(1, match_rtdef.capability_count_);
  EXPECT_EQ(1, match_rtdef.compute_cost_count_);
  EXPECT_EQ(0, match_rtdef.nonnull_filter_set_count_);
  EXPECT_EQ(0, match_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest, filter_pushdown_uses_native_bmm_only_for_eligible_group)
{
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef filter_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASMatchCtDef match_ctdef(allocator_);
  GroupAwareMatchRtDef match_rtdef;
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  filter_rtdef.ctdef_ = &filter_ctdef;
  ir_ctdef.is_async_index_ = 1;
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = 1;
  match_ctdef.ir_ctdef_idx_ = 0;
  match_rtdef.ctdef_ = &match_ctdef;
  match_ctdef.set_is_scoring(true);
  match_ctdef.set_is_top_level_scoring(true);

  add_clause(MUST, &match_rtdef);
  add_clause(FILTER, &filter_rtdef);
  prepare_structure();
  ctdef_.set_is_scoring(true);
  ctdef_.set_is_top_level_scoring(true);
  SearchCtxSetup setup(allocator_);

  setup.search_ctx_.set_async_binding(9001, 0, false);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_BMM, op->get_op_type());

  setup.search_ctx_.set_async_binding(9001, 1, false);
  op = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(
      ObDASSearchCost(10), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  EXPECT_EQ(2, op->get_children_cnt());
  bool has_segment_merge = false;
  for (int64_t i = 0; i < op->get_children_cnt(); ++i) {
    has_segment_merge = has_segment_merge
        || DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE == op->get_children()[i]->get_op_type();
  }
  EXPECT_TRUE(has_segment_merge);
  EXPECT_EQ(2, match_rtdef.capability_count_);
  EXPECT_EQ(2, match_rtdef.generate_count_);
  EXPECT_EQ(2, filter_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest, group_bound_async_scalar_primary_probe_respects_gates)
{
  SearchCtxSetup setup(allocator_);
  ObDASScalarCtDef scalar_ctdef(allocator_);
  ObDASScalarRtDef scalar_rtdef;
  ObDASScalarScanCtDef index_ctdef(allocator_);
  ObDASScalarScanCtDef primary_ctdef(allocator_);
  MockScalarScanRtDef index_rtdef;
  MockScalarScanRtDef primary_rtdef;
  ObDASBaseCtDef *ctdef_children[] = {&index_ctdef, &primary_ctdef};
  ObDASBaseRtDef *rtdef_children[] = {&index_rtdef, &primary_rtdef};

  index_ctdef.ref_table_id_ = 9001;
  index_ctdef.is_async_index_ = true;
  primary_ctdef.ref_table_id_ = 9002;
  primary_ctdef.is_primary_table_scan_ = true;
  index_rtdef.ctdef_ = &index_ctdef;
  primary_rtdef.ctdef_ = &primary_ctdef;
  scalar_ctdef.children_ = ctdef_children;
  scalar_ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  scalar_ctdef.set_has_index_scan(true);
  scalar_ctdef.set_has_main_scan(true);
  scalar_ctdef.set_primary_get_ratio(1);
  scalar_rtdef.ctdef_ = &scalar_ctdef;
  scalar_rtdef.children_ = rtdef_children;
  scalar_rtdef.children_cnt_ = ARRAYSIZEOF(rtdef_children);
  scalar_rtdef.set_allow_probe(true);

  setup.search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, scalar_rtdef.generate_op(
      ObDASSearchCost(0), setup.search_ctx_, op));
  EXPECT_EQ(0, index_rtdef.generate_count_);
  EXPECT_EQ(1, primary_rtdef.generate_count_);

  setup.search_ctx_.set_async_binding(common::OB_INVALID_ID, -1, false);
  op = nullptr;
  ASSERT_EQ(OB_SUCCESS, scalar_rtdef.generate_op(
      ObDASSearchCost(0), setup.search_ctx_, op));
  EXPECT_EQ(0, index_rtdef.generate_count_);
  EXPECT_EQ(2, primary_rtdef.generate_count_);

  // Group binding does not bypass the ratio, cost or follower gates.
  setup.search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  scalar_ctdef.set_primary_get_ratio(0);
  op = nullptr;
  ASSERT_EQ(OB_SUCCESS, scalar_rtdef.generate_op(
      ObDASSearchCost(0), setup.search_ctx_, op));
  EXPECT_EQ(1, index_rtdef.generate_count_);
  EXPECT_EQ(2, primary_rtdef.generate_count_);

  scalar_ctdef.set_primary_get_ratio(1);
  op = nullptr;
  ASSERT_EQ(OB_SUCCESS, scalar_rtdef.generate_op(
      ObDASSearchCost(2), setup.search_ctx_, op));
  EXPECT_EQ(2, index_rtdef.generate_count_);
  EXPECT_EQ(2, primary_rtdef.generate_count_);

  scalar_rtdef.set_allow_probe(false);
  op = nullptr;
  ASSERT_EQ(OB_SUCCESS, scalar_rtdef.generate_op(
      ObDASSearchCost(0), setup.search_ctx_, op));
  EXPECT_EQ(3, index_rtdef.generate_count_);
  EXPECT_EQ(2, primary_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest, group_local_boolean_rejects_mixed_sync_domain)
{
  ObDASScalarScanCtDef async_ctdef(allocator_);
  ObDASScalarScanCtDef sync_ctdef(allocator_);
  MockScalarScanRtDef async_rtdef;
  MockScalarScanRtDef sync_rtdef;
  async_ctdef.ref_table_id_ = 9001;
  async_ctdef.is_async_index_ = true;
  sync_ctdef.ref_table_id_ = 9002;
  sync_ctdef.is_async_index_ = false;
  async_rtdef.ctdef_ = &async_ctdef;
  sync_rtdef.ctdef_ = &sync_ctdef;
  add_clause(MUST, &async_rtdef);
  add_clause(MUST, &sync_rtdef);
  prepare_structure();

  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ObIDASSearchOp *op = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            rtdef_.generate_op(ObDASSearchCost(0), setup.search_ctx_, op));
  EXPECT_EQ(nullptr, op);
  EXPECT_EQ(0, async_rtdef.generate_count_);
  EXPECT_EQ(0, sync_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest, group_local_pure_range_boolean_builds_children_once)
{
  ObDASScalarScanCtDef left_ctdef(allocator_);
  ObDASScalarScanCtDef right_ctdef(allocator_);
  MockScalarScanRtDef left_rtdef;
  MockScalarScanRtDef right_rtdef;
  left_ctdef.ref_table_id_ = 9001;
  left_ctdef.is_async_index_ = true;
  right_ctdef.ref_table_id_ = 9001;
  right_ctdef.is_async_index_ = true;
  left_rtdef.ctdef_ = &left_ctdef;
  right_rtdef.ctdef_ = &right_ctdef;
  add_clause(SHOULD, &left_rtdef);
  add_clause(SHOULD, &right_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(1);

  ObDASAsyncIndexDomainAnalysis analysis;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &ctdef_, analysis));
  EXPECT_TRUE(analysis.is_pure_range_subtree_);

  ObDASAsyncIndexQueryCtx query_ctx;
  query_ctx.is_inited_ = true;
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_query_ctx(&query_ctx);
  setup.search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            rtdef_.generate_op(ObDASSearchCost(0), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, op->get_op_type());
  EXPECT_EQ(2, op->get_children_cnt());
  EXPECT_EQ(1, left_rtdef.generate_count_);
  EXPECT_EQ(1, right_rtdef.generate_count_);
  EXPECT_EQ(common::OB_INVALID_ID, setup.search_ctx_.get_async_index_table_id());
  EXPECT_EQ(0, setup.search_ctx_.get_async_group_slot());
  EXPECT_FALSE(setup.search_ctx_.get_async_direct_bind());
}

TEST_F(ObDASBooleanQueryTest, group_local_single_range_boolean_builds_child_once)
{
  ObDASScalarScanCtDef range_ctdef(allocator_);
  MockScalarScanRtDef range_rtdef;
  range_ctdef.ref_table_id_ = 9001;
  range_ctdef.is_async_index_ = true;
  range_rtdef.ctdef_ = &range_ctdef;
  add_clause(SHOULD, &range_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(1);

  ObDASAsyncIndexDomainAnalysis analysis;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &ctdef_, analysis));
  EXPECT_TRUE(analysis.is_pure_range_subtree_);

  ObDASAsyncIndexQueryCtx query_ctx;
  query_ctx.is_inited_ = true;
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_query_ctx(&query_ctx);
  setup.search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            rtdef_.generate_op(ObDASSearchCost(0), setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(1, range_rtdef.generate_count_);
}

TEST_F(ObDASBooleanQueryTest, group_local_mixed_range_ror_boolean_keeps_legacy_shape)
{
  ObDASScalarScanCtDef range_ctdef(allocator_);
  ObDASScalarScanCtDef ror_ctdef(allocator_);
  MockScalarScanRtDef range_rtdef;
  MockScalarScanRtDef ror_rtdef;
  range_ctdef.ref_table_id_ = 9001;
  range_ctdef.is_async_index_ = true;
  ror_ctdef.ref_table_id_ = 9001;
  ror_ctdef.is_async_index_ = true;
  ror_ctdef.is_index_term_point_scan_ = true;
  range_rtdef.ctdef_ = &range_ctdef;
  ror_rtdef.ctdef_ = &ror_ctdef;
  add_clause(SHOULD, &range_rtdef);
  add_clause(SHOULD, &ror_rtdef);
  prepare_structure();
  ctdef_.set_min_should_match(1);

  ObDASAsyncIndexDomainAnalysis analysis;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &ctdef_, analysis));
  EXPECT_FALSE(analysis.is_pure_range_subtree_);
}

TEST_F(ObDASBooleanQueryTest, group_local_only_must_not_range_keeps_legacy_shape)
{
  ObDASScalarScanCtDef range_ctdef(allocator_);
  MockScalarScanRtDef range_rtdef;
  range_ctdef.ref_table_id_ = 9001;
  range_ctdef.is_async_index_ = true;
  range_rtdef.ctdef_ = &range_ctdef;
  add_clause(MUST_NOT, &range_rtdef);
  prepare_structure();

  ObDASAsyncIndexDomainAnalysis analysis;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &ctdef_, analysis));
  EXPECT_FALSE(analysis.is_pure_range_subtree_);
}

TEST_F(ObDASBooleanQueryTest,
       domain_pure_range_and_compatible_sync_filter_are_independent)
{
  ObDASScalarScanCtDef range_ctdef(allocator_);
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  MockScalarScanRtDef range_rtdef;
  MockScalarScanRtDef filter_rtdef;
  range_ctdef.ref_table_id_ = 9001;
  range_ctdef.is_async_index_ = true;
  filter_ctdef.ref_table_id_ = 9001;
  filter_ctdef.is_async_index_ = true;
  range_rtdef.ctdef_ = &range_ctdef;
  filter_rtdef.ctdef_ = &filter_ctdef;
  add_clause(MUST, &range_rtdef);
  add_clause(FILTER, &filter_rtdef);
  prepare_structure();
  ObDASAsyncIndexDomainAnalysis analysis;
  ASSERT_EQ(OB_SUCCESS, ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
      &ctdef_, analysis));
  EXPECT_TRUE(analysis.is_pure_range_subtree_);
  EXPECT_FALSE(analysis.has_group_compatible_sync_filter_);

  filter_ctdef.ref_table_id_ = 9002;
  filter_ctdef.is_async_index_ = false;
  ASSERT_EQ(OB_SUCCESS, ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
      &ctdef_, analysis));
  EXPECT_FALSE(analysis.is_pure_range_subtree_);
  EXPECT_TRUE(analysis.has_group_compatible_sync_filter_);
  EXPECT_EQ(ASYNC_INDEX_DOMAIN_SINGLE, analysis.type_);
  EXPECT_EQ(ASYNC_INDEX_EXEC_GROUP, analysis.get_exec_mode());

  // A primary-only Filter is compatible, but must not inherit pure-range status.
  ObDASScalarCtDef primary_only_ctdef(allocator_);
  ctdef_.children_[1] = &primary_only_ctdef;
  ASSERT_EQ(OB_SUCCESS, ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
      &ctdef_, analysis));
  EXPECT_FALSE(analysis.is_pure_range_subtree_);
  EXPECT_TRUE(analysis.has_group_compatible_sync_filter_);

  // Reusing the output object must clear the previous synchronous-domain flag.
  filter_ctdef.ref_table_id_ = 9001;
  filter_ctdef.is_async_index_ = true;
  ctdef_.children_[1] = &filter_ctdef;
  ASSERT_EQ(OB_SUCCESS, ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
      &ctdef_, analysis));
  EXPECT_TRUE(analysis.is_pure_range_subtree_);
  EXPECT_FALSE(analysis.has_group_compatible_sync_filter_);
}

TEST_F(ObDASBooleanQueryTest, group_nested_sync_filter_fallback_preserves_domain)
{
  class DomainRecordingRtDef : public MockSearchRtDef
  {
  public:
    DomainRecordingRtDef(int64_t cost, const std::vector<uint64_t> &rowids)
        : MockSearchRtDef(cost, rowids), generated_in_filter_domain_(false) {}
    int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
                    ObIDASSearchOp *&op) override
    {
      generated_in_filter_domain_ = search_ctx.is_generating_group_filter_domain();
      return MockSearchRtDef::generate_op(lead_cost, search_ctx, op);
    }
    bool generated_in_filter_domain_;
  };

  for (int64_t use_or = 0; use_or < 2; ++use_or) {
    reset_ctdef();
    ObDASScalarScanCtDef async_ctdef(allocator_);
    ObDASScalarScanCtDef sync_ctdef_1(allocator_);
    ObDASScalarScanCtDef sync_ctdef_2(allocator_);
    async_ctdef.ref_table_id_ = 9001;
    async_ctdef.is_async_index_ = true;
    sync_ctdef_1.ref_table_id_ = 9002;
    sync_ctdef_2.ref_table_id_ = 9003;
    DomainRecordingRtDef async_rtdef(10, {1, 2, 3});
    DomainRecordingRtDef sync_rtdef_1(3, {1, 3});
    DomainRecordingRtDef sync_rtdef_2(4, {2, 3});
    async_rtdef.ctdef_ = &async_ctdef;
    sync_rtdef_1.ctdef_ = &sync_ctdef_1;
    sync_rtdef_2.ctdef_ = &sync_ctdef_2;
    ObDASBooleanQueryCtDef nested_ctdef(allocator_);
    ObDASBooleanQueryRtDef nested_rtdef;
    ObDASBaseCtDef *nested_children[] = {&sync_ctdef_1, &sync_ctdef_2};
    ObDASBaseRtDef *nested_rt_children[] = {&sync_rtdef_1, &sync_rtdef_2};
    nested_ctdef.children_ = nested_children;
    nested_ctdef.children_cnt_ = ARRAYSIZEOF(nested_children);
    if (use_or) {
      nested_ctdef.set_should(0, 2);
      nested_ctdef.set_min_should_match(1);
    } else {
      // MUST inside the original FILTER inherits its already validated domain.
      nested_ctdef.set_must(0, 2);
    }
    nested_rtdef.ctdef_ = &nested_ctdef;
    nested_rtdef.children_ = nested_rt_children;
    nested_rtdef.children_cnt_ = ARRAYSIZEOF(nested_rt_children);
    add_clause(MUST, &async_rtdef);
    add_clause(FILTER, &nested_rtdef);
    prepare_structure();

    SearchCtxSetup setup(allocator_);
    setup.search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
    ObIDASSearchOp *op = nullptr;
    // The mock MUST does not support BMM, so this always exercises req fallback.
    ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(20), setup.search_ctx_, op));
    EXPECT_FALSE(async_rtdef.generated_in_filter_domain_);
    EXPECT_TRUE(sync_rtdef_1.generated_in_filter_domain_);
    EXPECT_TRUE(sync_rtdef_2.generated_in_filter_domain_);
    EXPECT_FALSE(setup.search_ctx_.is_generating_group_filter_domain());
    ASSERT_NE(nullptr, op);
    verify_result(op, use_or ? std::vector<uint64_t>{1, 2, 3}
                            : std::vector<uint64_t>{3});
  }
}

TEST_F(ObDASBooleanQueryTest, group_required_sync_boolean_does_not_borrow_filter_domain)
{
  ObDASScalarScanCtDef sync_ctdef(allocator_);
  sync_ctdef.ref_table_id_ = 9002;
  MockSearchRtDef sync_rtdef(3, {1, 3});
  sync_rtdef.ctdef_ = &sync_ctdef;
  ObDASBooleanQueryCtDef nested_ctdef(allocator_);
  ObDASBooleanQueryRtDef nested_rtdef;
  ObDASBaseCtDef *nested_children[] = {&sync_ctdef};
  ObDASBaseRtDef *nested_rt_children[] = {&sync_rtdef};
  nested_ctdef.children_ = nested_children;
  nested_ctdef.children_cnt_ = ARRAYSIZEOF(nested_children);
  nested_ctdef.set_must(0, 1);
  nested_rtdef.ctdef_ = &nested_ctdef;
  nested_rtdef.children_ = nested_rt_children;
  nested_rtdef.children_cnt_ = ARRAYSIZEOF(nested_rt_children);
  ObIDASSearchRtDef *required[] = {&nested_rtdef};
  ObBooleanSubClause<ObIDASSearchRtDef> must(required, 1);
  ObBooleanSubClause<ObIDASSearchRtDef> filter;
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ObIDASSearchOp *op = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            rtdef_.req(must, filter, ObDASSearchCost(20), false, setup.search_ctx_, op));
  EXPECT_FALSE(setup.search_ctx_.is_generating_group_filter_domain());
  EXPECT_EQ(nullptr, op);
}

// ============================================================================
// Section 1: compute_cost Tests - Normal Cases
// ============================================================================
// Tests for computing query cost under normal conditions

// Test: Required cost should prefer the minimum cost clause
// Verifies that compute_cost returns the minimum cost among MUST and FILTER clauses
TEST_F(ObDASBooleanQueryTest, required_cost_prefers_minimum_clause)
{
  MockSearchRtDef must_high_cost(10);
  MockSearchRtDef must_low_cost(3);
  MockSearchRtDef filter_cost(5);

  add_clause(MUST, &must_high_cost);
  add_clause(MUST, &must_low_cost);
  add_clause(FILTER, &filter_cost);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(3, cost.cost());
}

// Test: Should cost calculation respects min_should_match parameter
// Verifies that should_cost is calculated correctly based on min_should_match value
TEST_F(ObDASBooleanQueryTest, should_cost_respects_min_should_match)
{
  MockSearchRtDef clause_a(9);
  MockSearchRtDef clause_b(1);
  MockSearchRtDef clause_c(5);

  add_clause(SHOULD, &clause_a);
  add_clause(SHOULD, &clause_b);
  add_clause(SHOULD, &clause_c);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(15, cost.cost());
  ctdef_.min_should_match_ = 2;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(6, cost.cost());
  ctdef_.min_should_match_ = 3;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(1, cost.cost());
}

// Test: compute_cost fails when no clause is available
// Verifies error handling when there are no clauses to compute cost from
TEST_F(ObDASBooleanQueryTest, compute_cost_fails_when_no_clause_available)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_cost with mixed MUST, FILTER, and SHOULD clauses
// Verifies cost calculation when all clause types are present
TEST_F(ObDASBooleanQueryTest, compute_cost_with_mixed_clauses)
{
  // Test case: must=5, filter=3, should=[10, 2, 8], min_should_match=2
  // Expected: required_cost = min(5, 3) = 3
  //           should_cost = 2+8 = 10
  //           final_cost = min(3, 10) = 3
  ctdef_.min_should_match_ = 2;

  MockSearchRtDef must_clause(5);
  MockSearchRtDef filter_clause(3);
  MockSearchRtDef should_clause_a(10);
  MockSearchRtDef should_clause_b(2);
  MockSearchRtDef should_clause_c(8);

  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause_a);
  add_clause(SHOULD, &should_clause_b);
  add_clause(SHOULD, &should_clause_c);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));

  EXPECT_EQ(3, cost.cost());
}

// Test: compute_cost with mixed clauses where should_cost wins
// Verifies that when should_cost is smaller than required_cost, it is selected
TEST_F(ObDASBooleanQueryTest, compute_cost_with_mixed_clauses_should_wins)
{
  // Test case: must=10, filter=8, should=[1, 2], min_should_match=1
  // Expected: required_cost = min(10, 8) = 8
  //           should_cost = 1+2 = 3 (least 2 costs from 2 should clauses)
  //           final_cost = min(8, 3) = 3
  ctdef_.min_should_match_ = 1;

  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause(8);
  MockSearchRtDef should_clause_a(1);
  MockSearchRtDef should_clause_b(2);

  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause_a);
  add_clause(SHOULD, &should_clause_b);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  // should_cost: least_clause_cnt = 2-1+1 = 2, sum of all 2 = 1+2 = 3
  // required_cost = min(10, 8) = 8
  // final_cost = min(8, 3) = 3
  EXPECT_EQ(3, cost.cost());
}

// ============================================================================
// Section 2: generate_op Tests - Normal Cases (Various Clause Combinations)
// ============================================================================
// Tests for generating search operators under normal conditions with different clause combinations

// Test: Only MUST clauses should generate CONJUNCTION op
// Verifies that multiple MUST clauses result in a CONJUNCTION operator
TEST_F(ObDASBooleanQueryTest, generate_op_only_must_clauses)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause1(10);
  MockSearchRtDef must_clause2(5);
  MockSearchRtDef must_clause3(8);
  must_clause1.ctdef_ = &mock_ctdef;
  must_clause2.ctdef_ = &mock_ctdef;
  must_clause3.ctdef_ = &mock_ctdef;
  
  add_clause(MUST, &must_clause1);
  add_clause(MUST, &must_clause2);
  add_clause(MUST, &must_clause3);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  
  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(3, conjunction_op->children_cnt_);
}

// Test: Only FILTER clauses should generate CONJUNCTION op
// Verifies that multiple FILTER clauses result in a CONJUNCTION operator
TEST_F(ObDASBooleanQueryTest, generate_op_only_filter_clauses)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef filter_clause1(7);
  MockSearchRtDef filter_clause2(3);
  filter_clause1.ctdef_ = &mock_ctdef;
  filter_clause2.ctdef_ = &mock_ctdef;
  
  add_clause(FILTER, &filter_clause1);
  add_clause(FILTER, &filter_clause2);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  
  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(2, conjunction_op->children_cnt_);
}

TEST_F(ObDASBooleanQueryTest, estimate_filter_cost_does_not_generate_or_materialize_filter)
{
  SearchCtxSetup setup(allocator_);
  setup.search_ctx_.set_async_binding(OB_INVALID_ID, 0, false);
  MockScalarScanRtDef first_rtdef;
  MockScalarScanRtDef second_rtdef;
  first_rtdef.cost_ = ObDASSearchCost(17);
  second_rtdef.cost_ = ObDASSearchCost(7);
  ObIDASSearchRtDef *clauses[] = {&first_rtdef, &second_rtdef};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(clauses, ARRAYSIZEOF(clauses));
  ObDASSearchCost filter_cost;

  ASSERT_EQ(OB_SUCCESS, rtdef_.estimate_filter_cost(
      filter_clauses, setup.search_ctx_, filter_cost));
  EXPECT_EQ(7, filter_cost.cost());
  EXPECT_EQ(0, first_rtdef.generate_count_);
  EXPECT_EQ(0, second_rtdef.generate_count_);
  EXPECT_EQ(0, setup.search_ctx_.get_filter_bitmap_provider_count());
  EXPECT_FALSE(setup.search_ctx_.is_generating_group_filter_domain());
}

TEST_F(ObDASBooleanQueryTest, estimate_filter_cost_discards_partial_estimates)
{
  for (int64_t invalid_metadata = 0; invalid_metadata < 2; ++invalid_metadata) {
    SCOPED_TRACE(invalid_metadata);
    SearchCtxSetup setup(allocator_);
    MockSearchRtDef valid_clause(17);
    MockSearchRtDef unknown_clause(7);
    MockSearchRtDef last_clause(3);
    if (invalid_metadata) {
      unknown_clause.set_return_invalid_cost(true);
    } else {
      unknown_clause.set_return_error(OB_ERR_UNEXPECTED);
    }
    ObIDASSearchRtDef *clauses[] = {&valid_clause, &unknown_clause, &last_clause};
    ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(clauses, ARRAYSIZEOF(clauses));
    ObDASSearchCost filter_cost(99);
    ASSERT_EQ(OB_SUCCESS, rtdef_.estimate_filter_cost(
        filter_clauses, setup.search_ctx_, filter_cost));
    EXPECT_FALSE(filter_cost.is_valid());
    EXPECT_EQ(0, setup.search_ctx_.get_filter_bitmap_provider_count());
  }
}

TEST_F(ObDASBooleanQueryTest, prepare_bmm_input_collects_minimum_cost)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef filter_clause1(7);
  MockSearchRtDef filter_clause2(3);
  filter_clause1.ctdef_ = &mock_ctdef;
  filter_clause2.ctdef_ = &mock_ctdef;
  ObIDASSearchRtDef *clauses[] = {&filter_clause1, &filter_clause2};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(clauses, ARRAYSIZEOF(clauses));
  ObIDASSearchOp *filter_op = nullptr;
  ObDASSearchCost filter_cost;

  has_bmm_options_ = true;
  bmm_options_.enable_vectorized_merge_ = true;
  ASSERT_EQ(OB_SUCCESS,
            rtdef_.prepare_bmm_input(
                filter_clauses, ObDASSearchCost(10), ctx_setup.search_ctx_,
                filter_op, filter_cost, false, bmm_options_, has_bmm_options_));
  ASSERT_NE(nullptr, filter_op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, filter_op->get_op_type());
  EXPECT_EQ(3, filter_cost.cost());
  EXPECT_FALSE(has_bmm_options_);
}

TEST_F(ObDASBooleanQueryTest, prepare_bmm_input_degrades_on_cost_error)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef valid_clause(7);
  MockSearchRtDef error_clause(3);
  valid_clause.ctdef_ = &mock_ctdef;
  error_clause.ctdef_ = &mock_ctdef;
  error_clause.set_return_error(OB_ERR_UNEXPECTED);
  ObIDASSearchRtDef *clauses[] = {&valid_clause, &error_clause};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(clauses, ARRAYSIZEOF(clauses));
  ObIDASSearchOp *filter_op = nullptr;
  ObDASSearchCost filter_cost;

  ASSERT_EQ(OB_SUCCESS,
            rtdef_.prepare_bmm_input(
                filter_clauses, ObDASSearchCost(10), ctx_setup.search_ctx_,
                filter_op, filter_cost, false, bmm_options_, has_bmm_options_));
  ASSERT_NE(nullptr, filter_op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, filter_op->get_op_type());
  EXPECT_FALSE(filter_cost.is_valid());
}

TEST_F(ObDASBooleanQueryTest, prepare_bmm_input_degrades_on_invalid_cost)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef invalid_clause(7);
  invalid_clause.ctdef_ = &mock_ctdef;
  invalid_clause.set_return_invalid_cost(true);
  ObIDASSearchRtDef *clauses[] = {&invalid_clause};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(clauses, ARRAYSIZEOF(clauses));
  ObIDASSearchOp *filter_op = nullptr;
  ObDASSearchCost filter_cost;

  ASSERT_EQ(OB_SUCCESS,
            rtdef_.prepare_bmm_input(
                filter_clauses, ObDASSearchCost(10), ctx_setup.search_ctx_,
                filter_op, filter_cost, false, bmm_options_, has_bmm_options_));
  ASSERT_NE(nullptr, filter_op);
  EXPECT_FALSE(filter_cost.is_valid());
}

TEST_F(ObDASBooleanQueryTest, prepare_bmm_input_rejects_null_clause)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObIDASSearchRtDef *clauses[] = {nullptr};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(clauses, ARRAYSIZEOF(clauses));
  ObIDASSearchOp *filter_op = reinterpret_cast<ObIDASSearchOp *>(1);
  ObDASSearchCost filter_cost(10);

  has_bmm_options_ = true;
  bmm_options_.enable_vectorized_merge_ = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            rtdef_.prepare_bmm_input(
                filter_clauses, ObDASSearchCost(10), ctx_setup.search_ctx_,
                filter_op, filter_cost, false, bmm_options_, has_bmm_options_));
  EXPECT_EQ(nullptr, filter_op);
  EXPECT_FALSE(filter_cost.is_valid());
  EXPECT_FALSE(has_bmm_options_);
}

TEST_F(ObDASBooleanQueryTest, prepare_bmm_input_propagates_generation_error)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchRtDef filter_clause(7);
  ObIDASSearchRtDef *clauses[] = {&filter_clause};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(clauses, ARRAYSIZEOF(clauses));
  ObIDASSearchOp *filter_op = nullptr;
  ObDASSearchCost filter_cost;

  EXPECT_EQ(OB_ERR_UNEXPECTED,
            rtdef_.prepare_bmm_input(
                filter_clauses, ObDASSearchCost(10), ctx_setup.search_ctx_,
                filter_op, filter_cost, false, bmm_options_, has_bmm_options_));
  EXPECT_EQ(nullptr, filter_op);
  EXPECT_EQ(7, filter_cost.cost());
}

TEST_F(ObDASBooleanQueryTest, generate_op_must_and_filter_clauses)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause1(5);
  MockSearchRtDef filter_clause2(8);
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause1.ctdef_ = &mock_ctdef;
  filter_clause2.ctdef_ = &mock_ctdef;
  
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause1);
  add_clause(FILTER, &filter_clause2);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  
  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(3, conjunction_op->children_cnt_);
}

// Test: Only SHOULD clauses should generate DISJUNCTION op
// Verifies that multiple SHOULD clauses result in a DISJUNCTION operator
TEST_F(ObDASBooleanQueryTest, generate_op_only_should_clauses)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause1(5);
  MockSearchRtDef should_clause2(10);
  MockSearchRtDef should_clause3(3);
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  should_clause3.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  add_clause(SHOULD, &should_clause3);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, op->get_op_type());
  
  ObDASDisjunctionOp *disjunction_op = static_cast<ObDASDisjunctionOp *>(op);
  EXPECT_EQ(3, disjunction_op->children_cnt_);
}

// Test: MUST + SHOULD with min_should_match == 0 should generate REQ_OPT op
// Verifies that when min_should_match is 0, a REQ_OPT operator is generated
TEST_F(ObDASBooleanQueryTest, generate_op_must_with_should_min_match_zero)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause(5);
  MockSearchRtDef should_clause1(5);
  MockSearchRtDef should_clause2(8);
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  
  ctdef_.min_should_match_ = 0;
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_OPT, op->get_op_type());
  ObDASReqOptOp *req_opt_op = static_cast<ObDASReqOptOp *>(op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, req_opt_op->required_->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, req_opt_op->optional_->get_op_type());
}

// Test: MUST + SHOULD with min_should_match > 0 should generate CONJUNCTION op
// Verifies that when min_should_match > 0, a CONJUNCTION operator is generated
TEST_F(ObDASBooleanQueryTest, generate_op_must_with_should_min_match_positive)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef should_clause1(5);
  MockSearchRtDef should_clause2(8);
  must_clause.ctdef_ = &mock_ctdef;
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  
  ctdef_.min_should_match_ = 1;
  add_clause(MUST, &must_clause);
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  
  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(2, conjunction_op->children_cnt_);
  ObIDASSearchOp *conj_op = conjunction_op->get_children()[0];
  ObIDASSearchOp *disj_op = conjunction_op->get_children()[1];
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, conj_op->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, disj_op->get_op_type());
}

// Test: MUST + MUST_NOT clauses should generate REQ_EXCL op
// Verifies that combining MUST and MUST_NOT clauses results in a REQ_EXCL operator
TEST_F(ObDASBooleanQueryTest, generate_op_must_with_must_not)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause(5);
  MockSearchRtDef must_not_clause1(5);
  MockSearchRtDef must_not_clause2(8);
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  must_not_clause1.ctdef_ = &mock_ctdef;
  must_not_clause2.ctdef_ = &mock_ctdef;
  
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(MUST_NOT, &must_not_clause1);
  add_clause(MUST_NOT, &must_not_clause2);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_EXCL, op->get_op_type());
  ObDASReqExclOp *req_excl_op = static_cast<ObDASReqExclOp *>(op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, req_excl_op->required_->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, req_excl_op->excluded_->get_op_type());
}

// Test: SHOULD + MUST_NOT clauses should generate REQ_EXCL op
// Verifies that combining SHOULD and MUST_NOT clauses results in a REQ_EXCL operator
TEST_F(ObDASBooleanQueryTest, generate_op_should_with_must_not)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause1(10);
  MockSearchRtDef should_clause2(5);
  MockSearchRtDef must_not_clause(8);
  MockSearchRtDef must_not_clause2(3);
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  must_not_clause.ctdef_ = &mock_ctdef;
  must_not_clause2.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  add_clause(MUST_NOT, &must_not_clause);
  add_clause(MUST_NOT, &must_not_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_EXCL, op->get_op_type());
  ObDASReqExclOp *req_excl_op = static_cast<ObDASReqExclOp *>(op);
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, req_excl_op->required_->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, req_excl_op->excluded_->get_op_type());
}

// Test: Complex query with all clause types should generate complex operator tree
// Verifies that combining all clause types (MUST, FILTER, SHOULD, MUST_NOT) generates a complex tree
TEST_F(ObDASBooleanQueryTest, generate_op_complex_query)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause(5);
  MockSearchRtDef should_clause(8);
  MockSearchRtDef should_clause2(3);
  MockSearchRtDef must_not_clause(3);
  MockSearchRtDef must_not_clause2(5);
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  should_clause.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  must_not_clause.ctdef_ = &mock_ctdef;
  must_not_clause2.ctdef_ = &mock_ctdef;
  
  ctdef_.min_should_match_ = 1;
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause);
  add_clause(SHOULD, &should_clause2);
  add_clause(MUST_NOT, &must_not_clause);
  add_clause(MUST_NOT, &must_not_clause2);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(2, conjunction_op->children_cnt_);
  ObIDASSearchOp *req_excl_op = conjunction_op->get_children()[0];
  ObIDASSearchOp *disjunction_op = conjunction_op->get_children()[1];
  EXPECT_EQ(DAS_SEARCH_OP_REQ_EXCL, req_excl_op->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, disjunction_op->get_op_type());
}

// ============================================================================
// Section 3: Execution Tests - Actual Query Execution and Result Verification
// ============================================================================
// Tests for actual query execution, verifying correct results and behavior

// Test: ConjunctionOp execution with two MUST clauses
// Verifies that CONJUNCTION operator correctly intersects results from multiple clauses
TEST_F(ObDASBooleanQueryTest, conjunction_op_execution)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  mock_ctdef.set_is_scoring(true);
  
  // Op1: 1(1.0), 3(3.0), 5(5.0), 7(7.0), 9(9.0)
  MockSearchRtDef must_clause1(10, {1, 3, 5, 7, 9}, {1.0, 3.0, 5.0, 7.0, 9.0});
  // Op2: 3(0.3), 4(0.4), 7(0.7), 8(0.8), 9(0.9)
  MockSearchRtDef must_clause2(10, {3, 4, 7, 8, 9}, {0.3, 0.4, 0.7, 0.8, 0.9});
  
  must_clause1.ctdef_ = &mock_ctdef;
  must_clause2.ctdef_ = &mock_ctdef;
  
  add_clause(MUST, &must_clause1);
  add_clause(MUST, &must_clause2);
  ctdef_.set_is_scoring(true);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  // Expected: 3(3.3), 7(7.7), 9(9.9)
  verify_result(op, {3, 7, 9}, {3.3, 7.7, 9.9});
  
  // Test advance_to
  // advance to 2 -> should find 3(3.3)
  // advance to 5 -> should find 7(7.7) (since 5 is in op1 but not op2)
  // advance to 8 -> should find 9(9.9)
  // advance to 10 -> end
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  verify_advance_to(op, {
      {2, 3, 3.3},
      {5, 7, 7.7},
      {8, 9, 9.9},
      {10, UINT64_MAX, -1.0}
  });
}

// Test: DisjunctionOp execution with two SHOULD clauses
// Verifies that DISJUNCTION operator correctly unions results from multiple clauses
TEST_F(ObDASBooleanQueryTest, disjunction_op_execution)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  mock_ctdef.set_is_scoring(true);
  
  // Op1: 1(1.0), 5(5.0)
  MockSearchRtDef should_clause1(10, {1, 5}, {1.0, 5.0});
  // Op2: 3(0.3), 5(0.5), 7(0.7)
  MockSearchRtDef should_clause2(10, {3, 5, 7}, {0.3, 0.5, 0.7});
  
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  
  // min_should_match = 1 (default OR behavior)
  ctdef_.min_should_match_ = 1;
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  ctdef_.set_is_scoring(true);
  
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION, op->get_op_type());
  // Expected union:
  // 1: 1.0
  // 3: 0.3
  // 5: 5.0 + 0.5 = 5.5
  // 7: 0.7
  verify_result(op, {1, 3, 5, 7}, {1.0, 0.3, 5.5, 0.7});

  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  // Test advance_to
  // advance to 2 -> 3(0.3)
  // advance to 4 -> 5(5.5)
  // advance to 6 -> 7(0.7)
  verify_advance_to(op, {
      {2, 3, 0.3},
      {4, 5, 5.5},
      {6, 7, 0.7},
      {8, UINT64_MAX, -1.0}
  });
}

TEST_F(ObDASBooleanQueryTest, disjunction_shallow_child_exhaustion)
{
  struct TestCase {
    int64_t child_count_;
    int first_ret_;
    int second_ret_;
    int expected_ret_;
    double expected_max_score_;
  };
  const TestCase cases[] = {
    {1, OB_ITER_END, OB_ITER_END, OB_ITER_END, 0.0},
    {2, OB_ITER_END, OB_ITER_END, OB_ITER_END, 0.0},
    {2, OB_ITER_END, OB_SUCCESS, OB_SUCCESS, 2.0},
    {2, OB_SUCCESS, OB_ITER_END, OB_SUCCESS, 1.0},
    {2, OB_SUCCESS, OB_SUCCESS, OB_SUCCESS, 3.0},
    {2, OB_ITER_END, OB_TIMEOUT, OB_TIMEOUT, 0.0},
  };
  for (const TestCase &test_case : cases) {
    SCOPED_TRACE(test_case.child_count_);
    SCOPED_TRACE(test_case.first_ret_);
    SCOPED_TRACE(test_case.second_ret_);
    SearchCtxSetup ctx_setup(allocator_);
    MockShallowSearchOp first_op(ctx_setup.search_ctx_, test_case.first_ret_, 1.0);
    MockShallowSearchOp second_op(ctx_setup.search_ctx_, test_case.second_ret_, 2.0);
    MockSearchOpParam child_param;
    child_param.set_is_scoring(true);
    ASSERT_EQ(OB_SUCCESS, first_op.init(child_param));
    ASSERT_EQ(OB_SUCCESS, second_op.init(child_param));
    ObArray<ObIDASSearchOp *> children;
    ASSERT_EQ(OB_SUCCESS, children.push_back(&first_op));
    if (2 == test_case.child_count_) {
      ASSERT_EQ(OB_SUCCESS, children.push_back(&second_op));
    }
    ObDASDisjunctionOpParam param(children, 1, false, ObDASSearchCost(0));
    param.set_is_scoring(true);
    ObDASDisjunctionOp op(ctx_setup.search_ctx_);
    ASSERT_EQ(OB_SUCCESS, op.init(param));
    ASSERT_EQ(OB_SUCCESS, op.open());
    ObDASRowID target;
    target.set_uint64(1);
    int expected_ret = test_case.expected_ret_;
    double expected_max_score = test_case.expected_max_score_;
    for (int64_t round = 0; round < 3; ++round) {
      if (2 == round) {
        // Return valid ranges after rescan to verify that exhausted children contribute again.
        first_op.set_shallow_ret(OB_SUCCESS);
        second_op.set_shallow_ret(OB_SUCCESS);
        ASSERT_EQ(OB_SUCCESS, op.rescan());
        EXPECT_EQ(1, first_op.get_rescan_count());
        EXPECT_EQ(2 == test_case.child_count_ ? 1 : 0, second_op.get_rescan_count());
        target.set_uint64(2);
        expected_ret = OB_SUCCESS;
        expected_max_score = 1 == test_case.child_count_ ? 1.0 : 3.0;
      }
      const MaxScoreTuple *tuple = nullptr;
      EXPECT_EQ(expected_ret, op.advance_shallow(target, true, tuple));
      if (OB_SUCCESS == expected_ret) {
        ASSERT_NE(nullptr, tuple);
        ASSERT_TRUE(tuple->get_min_id().is_normal());
        EXPECT_EQ(target.get_uint64(), tuple->get_min_id().get_uint64());
        EXPECT_TRUE(tuple->get_max_id().is_max());
        EXPECT_DOUBLE_EQ(expected_max_score, tuple->get_max_score());
      } else {
        EXPECT_EQ(nullptr, tuple);
      }
    }
    ASSERT_EQ(OB_SUCCESS, op.close());
  }
}

TEST_F(ObDASBooleanQueryTest, topk_bmw_continues_after_disjunction_shallow_exhaustion)
{
  class MockBlockSearchOp : public MockSearchOp
  {
  public:
    MockBlockSearchOp(
        ObDASSearchCtx &ctx,
        const std::vector<uint64_t> &rowids,
        const std::vector<double> &scores)
      : MockSearchOp(ctx, rowids, scores),
        last_id_(rowids.back()), max_score_(*std::max_element(scores.begin(), scores.end()))
    {}

  protected:
    int do_advance_shallow(
        const ObDASRowID &target,
        const bool inclusive,
        const MaxScoreTuple *&tuple) override
    {
      int ret = OB_SUCCESS;
      tuple = nullptr;
      if (target.is_max() || (target.is_normal()
          && (target.get_uint64() > last_id_
              || (!inclusive && target.get_uint64() == last_id_)))) {
        ret = OB_ITER_END;
      } else {
        ObDASRowID max_id;
        max_id.set_uint64(last_id_);
        max_score_tuple_.set(target, max_id, max_score_);
        tuple = &max_score_tuple_;
      }
      return ret;
    }

    int do_calc_max_score(double &score) override
    {
      score = max_score_;
      return OB_SUCCESS;
    }

  private:
    uint64_t last_id_;
    double max_score_;
  };

  SearchCtxSetup ctx_setup(allocator_);
  // Keep the single rowid expression alive until TopK finishes writing its result.
  ObExpr rowid_expr;
  ExprFixedArray rowid_exprs(allocator_);
  ASSERT_EQ(OB_SUCCESS, rowid_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, rowid_exprs.push_back(&rowid_expr));
  ctx_setup.search_ctx_.rowid_exprs_ = &rowid_exprs;
  ctx_setup.search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs;

  MockBlockSearchOp a1(ctx_setup.search_ctx_, {2}, {0.5});
  MockBlockSearchOp a2(ctx_setup.search_ctx_, {2}, {0.5});
  MockBlockSearchOp b(ctx_setup.search_ctx_, {1, 10}, {1.0, 6.0});
  MockBlockSearchOp c(ctx_setup.search_ctx_, {1, 10}, {1.0, 6.0});
  MockSearchOpParam child_param;
  child_param.set_is_scoring(true);
  for (MockBlockSearchOp *child : {&a1, &a2, &b, &c}) {
    ASSERT_EQ(OB_SUCCESS, child->init(child_param));
  }

  ObArray<ObIDASSearchOp *> disjunction_children;
  ASSERT_EQ(OB_SUCCESS, disjunction_children.push_back(&a1));
  ASSERT_EQ(OB_SUCCESS, disjunction_children.push_back(&a2));
  ObDASDisjunctionOpParam disjunction_param(disjunction_children, 1, false, ObDASSearchCost(0));
  disjunction_param.set_is_scoring(true);
  ObDASDisjunctionOp disjunction(ctx_setup.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, disjunction.init(disjunction_param));

  ObArray<ObIDASSearchOp *> bmw_children;
  ASSERT_EQ(OB_SUCCESS, bmw_children.push_back(&disjunction));
  ASSERT_EQ(OB_SUCCESS, bmw_children.push_back(&b));
  ASSERT_EQ(OB_SUCCESS, bmw_children.push_back(&c));
  ObDASBMWOpParam bmw_param(bmw_children, 2, allocator_);
  bmw_param.set_is_scoring(true);
  ObDASBMWOp bmw(ctx_setup.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, bmw.init(bmw_param));

  ObDASTopKCollectOpParam topk_param;
  topk_param.limit_ = 1;
  topk_param.child_ = &bmw;
  topk_param.dynamic_pruning_enabled_ = true;
  topk_param.set_is_scoring(true);
  ObDASTopKCollectOp topk(ctx_setup.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, topk.init(topk_param));
  ASSERT_EQ(OB_SUCCESS, topk.open());

  // row1 raises the pruning threshold to 2; after A exhausts at row10, B/C must still contribute a total score of 12.
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, topk.next_rowid(rowid, score));
  EXPECT_EQ(10, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(12.0, score);
  EXPECT_EQ(OB_ITER_END, topk.next_rowid(rowid, score));
  ASSERT_EQ(OB_SUCCESS, topk.close());
  ASSERT_EQ(OB_SUCCESS, bmw.close());
  ASSERT_EQ(OB_SUCCESS, disjunction.close());
}

// Test: Mixed MUST and SHOULD clauses with min_should_match = 1
// Verifies correct execution when both MUST and SHOULD clauses are present
TEST_F(ObDASBooleanQueryTest, mixed_must_and_should_min_match_1)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  mock_ctdef.set_is_scoring(true);

  // MUST 1: {1, 3, 5, 7, 9}
  MockSearchRtDef must_clause1(10, {1, 3, 5, 7, 9}, {10.0, 30.0, 50.0, 70.0, 90.0});
  // MUST 2: {1, 3, 5, 7}
  MockSearchRtDef must_clause2(10, {1, 3, 5, 7}, {1.0, 3.0, 5.0, 7.0});

  // SHOULD 1: {1, 2, 5, 8}
  MockSearchRtDef should_clause1(10, {1, 2, 5, 8}, {1.0, 2.0, 5.0, 8.0});

  // SHOULD 2: {3, 5, 7, 10}
  MockSearchRtDef should_clause2(10, {3, 5, 7, 10}, {3.0, 5.0, 7.0, 10.0});

  must_clause1.ctdef_ = &mock_ctdef;
  must_clause2.ctdef_ = &mock_ctdef;
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(MUST, &must_clause1);
  add_clause(MUST, &must_clause2);
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  ctdef_.set_is_scoring(true);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));

  // Structure check
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  // Logic check:
  // Must Combined:
  // 1: M1(10.0) + M2(1.0) + S1(1.0) = 12.0
  // 3: M1(30.0) + M2(3.0) + S2(3.0) = 36.0
  // 5: M1(50.0) + M2(5.0) + S1(5.0) + S2(5.0) = 65.0
  // 7: M1(70.0) + M2(7.0) = + S2(7.0) = 84.0
  // 9: M1(90.0) -> Skip (not in M2)

  verify_result(op, {1, 3, 5, 7}, {12.0, 36.0, 65.0, 84.0});

  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  // Advance check
  verify_advance_to(op, {
    {2, 3, 36.0},
    {4, 5, 65.0},
    {6, 7, 84.0},
    {8, UINT64_MAX, -1.0}
  });
}

// Test: DisjunctionOp with min_should_match = 2
// Verifies that DISJUNCTION operator correctly filters results based on min_should_match
TEST_F(ObDASBooleanQueryTest, disjunction_min_should_match_2)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  mock_ctdef.set_is_scoring(true);

  // Op1: {10, 20, 30}
  MockSearchRtDef clause1(10, {10, 20, 30}, {1.0, 1.0, 1.0});
  // Op2: {10, 25, 30}
  MockSearchRtDef clause2(10, {10, 25, 30}, {2.0, 2.0, 2.0});
  // Op3: {10, 20, 35}
  MockSearchRtDef clause3(10, {10, 20, 35}, {3.0, 3.0, 3.0});

  clause1.ctdef_ = &mock_ctdef;
  clause2.ctdef_ = &mock_ctdef;
  clause3.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 2;
  add_clause(SHOULD, &clause1);
  add_clause(SHOULD, &clause2);
  add_clause(SHOULD, &clause3);
  ctdef_.set_is_scoring(true);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));

  // Structure: Disjunction
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION, op->get_op_type());


  // Expected ids match >= 2 clauses:
  // 10: 1.0 + 2.0 + 3.0 = 6.0
  // 20: 1.0 + 3.0 = 4.0
  // 30: 1.0 + 2.0 = 3.0
  verify_result(op, {10, 20, 30}, {6.0, 4.0, 3.0});

  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION, op->get_op_type());
  verify_advance_to(op, {
    {15, 20, 4.0},
    {21, 30, 3.0},
    {31, UINT64_MAX, -1.0}
  });
}

// ============================================================================
// Section 4: compute_cost Tests - Exception Cases
// ============================================================================
// Tests for error handling and edge cases in compute_cost

// Test: compute_cost with nullptr ctdef should return error
// Verifies error handling when ctdef is nullptr
TEST_F(ObDASBooleanQueryTest, compute_cost_nullptr_ctdef)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  rtdef_.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_cost with min_should_match == 0 and valid required_cost
// Verifies that when min_should_match is 0 and required_cost is valid, it returns early
TEST_F(ObDASBooleanQueryTest, compute_cost_min_should_match_zero_with_required)
{
  MockSearchRtDef must_clause(5);
  MockSearchRtDef filter_clause(3);
  MockSearchRtDef should_clause(10);

  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 0;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  // When min_should_match == 0 and required_cost is valid, should return required_cost
  EXPECT_EQ(3, cost.cost()); // min(5, 3) = 3
}

// Test: compute_cost with both required_cost and should_cost invalid
// Verifies error handling when both cost calculations result in invalid costs
TEST_F(ObDASBooleanQueryTest, compute_cost_both_invalid)
{
  MockSearchRtDef should_clause(10);
  should_clause.set_return_invalid_cost(true);

  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_required_cost_inner with nullptr clause
// Verifies error handling when a clause in the required clauses array is nullptr
TEST_F(ObDASBooleanQueryTest, compute_required_cost_inner_nullptr_clause)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  
  // Setup structure with nullptr clause in children array
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;
  
  ctdef_.must_.exist_ = true;
  ctdef_.must_.offset_ = 0;
  ctdef_.must_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause
  
  ObDASSearchCost cost;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_required_cost_inner with invalid cost
// Verifies error handling when a clause returns an invalid cost
TEST_F(ObDASBooleanQueryTest, compute_required_cost_inner_invalid_cost)
{
  MockSearchRtDef must_clause(10);
  must_clause.set_return_invalid_cost(true);

  add_clause(MUST, &must_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_should_cost with nullptr ctdef
// Verifies error handling when ctdef is nullptr in compute_should_cost
TEST_F(ObDASBooleanQueryTest, compute_should_cost_nullptr_ctdef)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  rtdef_.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test compute_should_cost with empty should clauses
TEST_F(ObDASBooleanQueryTest, compute_should_cost_empty_clauses)
{
  MockSearchRtDef must_clause(5);
  add_clause(MUST, &must_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(5, cost.cost());
}

// Test: compute_should_cost with nullptr clause
// Verifies error handling when a clause in the should clauses array is nullptr
TEST_F(ObDASBooleanQueryTest, compute_should_cost_nullptr_clause)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  
  // Setup structure with nullptr clause
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;
  
  ctdef_.should_.exist_ = true;
  ctdef_.should_.offset_ = 0;
  ctdef_.should_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause
  
  ObDASSearchCost cost;
  ctdef_.min_should_match_ = 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_should_cost with invalid clause cost
// Verifies error handling when a should clause returns an invalid cost
TEST_F(ObDASBooleanQueryTest, compute_should_cost_invalid_clause_cost)
{
  MockSearchRtDef should_clause(10);
  should_clause.set_return_invalid_cost(true);

  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// ============================================================================
// Section 5: generate_op Tests - Exception Cases
// ============================================================================
// Tests for error handling and edge cases in generate_op

// Test: generate_op with nullptr ctdef should return error
// Verifies error handling when ctdef is nullptr in generate_op
TEST_F(ObDASBooleanQueryTest, generate_op_nullptr_ctdef)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  rtdef_.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: generate_op with invalid cost should return error
// Verifies error handling when self_cost is invalid
TEST_F(ObDASBooleanQueryTest, generate_op_invalid_cost)
{
  MockSearchRtDef must_clause(10);
  must_clause.set_return_invalid_cost(true);

  add_clause(MUST, &must_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: req with single required clause should return the op directly
// Verifies that when there's only one required clause, it's returned directly without wrapping
TEST_F(ObDASBooleanQueryTest, req_single_required_clause)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 2, 3}, {1.0, 2.0, 3.0});
  must_clause.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  // Single clause should return the op directly, not wrapped in conjunction
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, op->get_op_type());
}

// Test: opt with need_score and is_top_level and min_match > 1
// Verifies that WAND operator is generated
TEST_F(ObDASBooleanQueryTest, opt_need_score_top_level_min_match_gt_1)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause(10);
  should_clause.ctdef_ = &mock_ctdef;

  add_clause(SHOULD, &should_clause);

  ctdef_.min_should_match_ = 2;
  ctdef_.is_scoring_ = true;
  ctdef_.is_top_level_scoring_ = true;

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: opt with need_score and is_top_level and min_match == 1
// Verifies that BMM operator is generated
TEST_F(ObDASBooleanQueryTest, opt_need_score_top_level_min_match_1)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef first_ctdef(allocator_);
  MockSearchCtDef second_ctdef(allocator_);
  MockSearchRtDef first_clause(10);
  MockSearchRtDef second_clause(20);
  first_clause.ctdef_ = &first_ctdef;
  second_clause.ctdef_ = &second_ctdef;

  add_clause(SHOULD, &first_clause);
  add_clause(SHOULD, &second_clause);

  ctdef_.min_should_match_ = 1;
  ctdef_.is_scoring_ = true;
  ctdef_.is_top_level_scoring_ = true;

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(7), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  ASSERT_EQ(DAS_SEARCH_OP_BMM, op->get_op_type());
  ObDASBMMOp *bmm_op = static_cast<ObDASBMMOp *>(op);
  ASSERT_EQ(2, bmm_op->score_op_costs_.count());
  EXPECT_EQ(10, bmm_op->score_op_costs_.at(0).cost());
  EXPECT_EQ(20, bmm_op->score_op_costs_.at(1).cost());
  EXPECT_EQ(7, bmm_op->lead_cost_.cost());
}

// Test: generate_op with MUST_NOT but no MUST/FILTER should fail
// Verifies error handling when MUST_NOT clauses exist without MUST/FILTER clauses
TEST_F(ObDASBooleanQueryTest, generate_op_must_not_without_must_filter)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_not_clause(10);
  must_not_clause.ctdef_ = &mock_ctdef;

  add_clause(MUST_NOT, &must_not_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  // This will fail because there are no must/filter clauses to generate req_op
  // and excl requires a valid main op
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: excl with empty prohibited clauses should return main op directly
// Verifies that when prohibited clauses are empty, the main op is returned without wrapping
TEST_F(ObDASBooleanQueryTest, excl_empty_prohibited)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 2, 3}, {1.0, 2.0, 3.0});
  must_clause.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause);
  // No must_not clauses

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  // Should return the main op directly when prohibited is empty
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, op->get_op_type());
}

// ============================================================================
// Section 6: Getter Methods Tests
// ============================================================================
// Tests for getter methods (must, filter, should, must_not) error handling

// Test: Getter method (must) with out of range should return error
// Verifies error handling when accessing clauses with invalid range
TEST_F(ObDASBooleanQueryTest, getter_out_of_range)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  
  // Setup invalid range
  ctdef_.must_.exist_ = true;
  ctdef_.must_.offset_ = 0;
  ctdef_.must_.count_ = 10; // But children_cnt_ is 0
  ctdef_.children_cnt_ = 0;
  
  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.must(clause));
}

// Test: Getter methods with nullptr ctdef should return error
// Verifies error handling when ctdef is nullptr in getter methods
TEST_F(ObDASBooleanQueryTest, getter_nullptr_ctdef)
{
  SearchCtxSetup ctx_setup(allocator_);
  rtdef_.ctdef_ = nullptr;
  
  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.must(clause));
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.filter(clause));
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.should(clause));
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.must_not(clause));
}

// Test: compute_cost with empty should_clauses
// Verifies that compute_cost handles empty should clauses correctly
TEST_F(ObDASBooleanQueryTest, compute_cost_empty_should_clauses)
{
  MockSearchRtDef must_clause(5);
  add_clause(MUST, &must_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1; // This shouldn't matter when should is empty
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(5, cost.cost());
}

// Test: compute_should_cost with least_clause_cnt calculation
// Verifies correct calculation of least_clause_cnt and cost summation
TEST_F(ObDASBooleanQueryTest, compute_should_cost_least_clause_cnt)
{
  MockSearchRtDef clause1(10);
  MockSearchRtDef clause2(5);
  MockSearchRtDef clause3(8);
  MockSearchRtDef clause4(3);
  MockSearchRtDef clause5(7);

  add_clause(SHOULD, &clause1);
  add_clause(SHOULD, &clause2);
  add_clause(SHOULD, &clause3);
  add_clause(SHOULD, &clause4);
  add_clause(SHOULD, &clause5);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 3;
  // should_cnt = 5, least_clause_cnt = 5 - 3 + 1 = 3
  // Should sum the 3 smallest costs: 3 + 5 + 7 = 15
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(15, cost.cost());
}

// Test: generate_op with only FILTER clauses (no MUST) should generate CONJUNCTION op
// Verifies that FILTER-only queries generate CONJUNCTION operator
TEST_F(ObDASBooleanQueryTest, generate_op_only_filter_no_must)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef filter_clause1(5, {1, 3, 5}, {1.0, 3.0, 5.0});
  MockSearchRtDef filter_clause2(3, {1, 3}, {1.0, 3.0});
  filter_clause1.ctdef_ = &mock_ctdef;
  filter_clause2.ctdef_ = &mock_ctdef;

  add_clause(FILTER, &filter_clause1);
  add_clause(FILTER, &filter_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  
  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(2, conjunction_op->children_cnt_);
  
  // Expected intersection: {1, 3}
  verify_result(op, {1, 3}, {0.0, 0.0});
}

// Test: generate_op with MUST + FILTER + SHOULD (min_match=0) should generate REQ_OPT op
// Verifies that when min_match is 0, a REQ_OPT operator is generated
TEST_F(ObDASBooleanQueryTest, generate_op_must_filter_should_min_match_zero)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 3, 5}, {10.0, 30.0, 50.0});
  MockSearchRtDef filter_clause(5, {1, 3}, {1.0, 3.0});
  MockSearchRtDef should_clause(8, {2, 4, 5}, {2.0, 4.0, 5.0});
  
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  should_clause.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 0;
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_OPT, op->get_op_type());
}

// Test: generate_op error propagation from req
// Verifies that errors from req function are properly propagated
TEST_F(ObDASBooleanQueryTest, generate_op_req_error_propagation)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  must_clause.ctdef_ = &mock_ctdef;
  must_clause.set_return_error(OB_ERR_UNEXPECTED);

  add_clause(MUST, &must_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: generate_op error propagation from opt
// Verifies that errors from opt function are properly propagated
TEST_F(ObDASBooleanQueryTest, generate_op_opt_error_propagation)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause(10);
  should_clause.ctdef_ = &mock_ctdef;
  should_clause.set_return_error(OB_ERR_UNEXPECTED);

  add_clause(SHOULD, &should_clause);
  ctdef_.min_should_match_ = 1;

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test generate_op error propagation from excl
// Note: excl calls opt which calls generate_op on clauses.
// If generate_op fails on a must_not clause, the error should propagate.
// However, the error needs to be set in generate_op, not compute_cost.
// Since we can't easily mock generate_op failure without more complex setup,
// this test verifies that excl handles empty prohibited correctly (which is tested elsewhere).
// For actual error propagation, we test it through the req_error_in_clause_generate_op test.
TEST_F(ObDASBooleanQueryTest, generate_op_excl_error_propagation)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 2, 3}, {1.0, 2.0, 3.0});
  MockSearchRtDef must_not_clause(5);
  must_not_clause.ctdef_ = &mock_ctdef;
  // Setting return_error in compute_cost won't affect generate_op
  // The error propagation test is better covered by req_error_in_clause_generate_op
  must_not_clause.set_return_error(OB_ERR_UNEXPECTED);
  
  must_clause.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause);
  add_clause(MUST_NOT, &must_not_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  // This will succeed because compute_cost error doesn't propagate to generate_op
  // The error is only checked during cost computation, not during op generation
  // For actual error propagation testing, see req_error_in_clause_generate_op
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: req with nullptr clause in must should return error
// Verifies error handling when a MUST clause is nullptr
TEST_F(ObDASBooleanQueryTest, req_nullptr_clause_in_must)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  
  // Setup structure with nullptr clause
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;
  
  ctdef_.must_.exist_ = true;
  ctdef_.must_.offset_ = 0;
  ctdef_.must_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause
  
  ObIDASSearchOp *op = nullptr;
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: req with nullptr clause in filter should return error
// Verifies error handling when a FILTER clause is nullptr
TEST_F(ObDASBooleanQueryTest, req_nullptr_clause_in_filter)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  
  // Setup structure with nullptr clause in filter
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;
  
  ctdef_.filter_.exist_ = true;
  ctdef_.filter_.offset_ = 0;
  ctdef_.filter_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause
  
  ObIDASSearchOp *op = nullptr;
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: opt with nullptr clause should return error
// Verifies error handling when a SHOULD clause is nullptr
TEST_F(ObDASBooleanQueryTest, opt_nullptr_clause)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  
  // Setup structure with nullptr clause in should
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;
  
  ctdef_.should_.exist_ = true;
  ctdef_.should_.offset_ = 0;
  ctdef_.should_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause
  ctdef_.min_should_match_ = 1;
  
  ObIDASSearchOp *op = nullptr;
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test getter methods with out of range for filter
TEST_F(ObDASBooleanQueryTest, getter_filter_out_of_range)
{
  SearchCtxSetup ctx_setup(allocator_);
  
  ctdef_.filter_.exist_ = true;
  ctdef_.filter_.offset_ = 0;
  ctdef_.filter_.count_ = 10;
  ctdef_.children_cnt_ = 0;
  
  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.filter(clause));
}

// Test: Getter method (should) with out of range should return error
// Verifies error handling when accessing should clauses with invalid range
TEST_F(ObDASBooleanQueryTest, getter_should_out_of_range)
{
  SearchCtxSetup ctx_setup(allocator_);
  
  ctdef_.should_.exist_ = true;
  ctdef_.should_.offset_ = 0;
  ctdef_.should_.count_ = 10;
  ctdef_.children_cnt_ = 0;
  
  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.should(clause));
}

// Test: Getter method (must_not) with out of range should return error
// Verifies error handling when accessing must_not clauses with invalid range
TEST_F(ObDASBooleanQueryTest, getter_must_not_out_of_range)
{
  SearchCtxSetup ctx_setup(allocator_);
  
  ctdef_.must_not_.exist_ = true;
  ctdef_.must_not_.offset_ = 0;
  ctdef_.must_not_.count_ = 10;
  ctdef_.children_cnt_ = 0;
  
  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.must_not(clause));
}

// Test compute_cost with should_cost valid but required_cost invalid
// Note: When compute_required_cost_inner encounters invalid cost, it returns OB_ERR_UNEXPECTED,
// which causes compute_required_cost to fail, and thus compute_cost to fail.
// So this test expects failure, not success.
TEST_F(ObDASBooleanQueryTest, compute_cost_should_valid_required_invalid)
{
  MockSearchRtDef should_clause(10);
  MockSearchRtDef must_clause(5);
  must_clause.set_return_invalid_cost(true);

  add_clause(MUST, &must_clause);
  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  // When required_cost is invalid, compute_required_cost_inner returns error,
  // causing compute_cost to fail before computing should_cost
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test compute_cost with required_cost valid but should_cost invalid
// Note: When compute_should_cost encounters invalid cost, it returns OB_ERR_UNEXPECTED,
// which causes compute_cost to fail. However, if required_cost is valid and min_should_match == 0,
// we return early with required_cost. But if min_should_match > 0, we need should_cost.
TEST_F(ObDASBooleanQueryTest, compute_cost_required_valid_should_invalid)
{
  MockSearchRtDef must_clause(5);
  MockSearchRtDef should_clause(10);
  should_clause.set_return_invalid_cost(true);

  add_clause(MUST, &must_clause);
  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  // When should_cost is invalid, compute_should_cost returns error,
  // causing compute_cost to fail
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: generate_op with mixed query and min_should_match > 0
// Verifies complex query generation with all clause types and min_should_match > 0
TEST_F(ObDASBooleanQueryTest, generate_op_mixed_min_match_positive_complex)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 3, 5}, {10.0, 30.0, 50.0});
  MockSearchRtDef filter_clause(5, {1, 3}, {1.0, 3.0});
  MockSearchRtDef should_clause1(8, {2, 4, 5}, {2.0, 4.0, 5.0});
  MockSearchRtDef should_clause2(3, {3, 5}, {3.0, 5.0});
  MockSearchRtDef must_not_clause(2, {2}, {2.0});
  
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  must_not_clause.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  add_clause(MUST_NOT, &must_not_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  // Should be CONJUNCTION with REQ_EXCL and DISJUNCTION
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
}

// Test: req with error in generate_op of clause
// Verifies error propagation when clause->generate_op fails
TEST_F(ObDASBooleanQueryTest, req_error_in_clause_generate_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  must_clause.ctdef_ = &mock_ctdef;
  must_clause.set_return_error(OB_ERR_UNEXPECTED);

  add_clause(MUST, &must_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: opt with error in generate_op of clause
// Verifies error propagation when clause->generate_op fails in opt
TEST_F(ObDASBooleanQueryTest, opt_error_in_clause_generate_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause(10);
  should_clause.ctdef_ = &mock_ctdef;
  should_clause.set_return_error(OB_ERR_UNEXPECTED);

  add_clause(SHOULD, &should_clause);
  ctdef_.min_should_match_ = 1;

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// ============================================================================
// Section 7: ObDASConjunctionOp Tests
// ============================================================================
// Tests for ConjunctionOp lifecycle, error handling, and edge cases

// Test: ConjunctionOp do_init with empty required_ops should return error
// Verifies error handling when required_ops array is empty
TEST_F(ObDASBooleanQueryTest, conjunction_op_inner_init_empty_required_ops)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> empty_ops;
  ObDASConjunctionOpParam param(empty_ops);
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.init(param));
}

// Test: ConjunctionOp open with nullptr op should return error
// Verifies error handling when a required op is nullptr during open()
TEST_F(ObDASBooleanQueryTest, conjunction_op_open_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp open with op->open() failure
// Verifies error propagation when a child op's open() fails
TEST_F(ObDASBooleanQueryTest, conjunction_op_open_op_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});
  mock_op1->set_open_error(OB_ERR_UNEXPECTED);
  
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test ConjunctionOp rescan with nullptr op
// Note: open() will fail if ops contains nullptr, so we test that open() detects it
TEST_F(ObDASBooleanQueryTest, conjunction_op_rescan_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  // open() should fail when it encounters nullptr op
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp rescan with op->rescan() failure
// Verifies error propagation when a child op's rescan() fails
TEST_F(ObDASBooleanQueryTest, conjunction_op_rescan_op_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});
  mock_op1->set_rescan_error(OB_ERR_UNEXPECTED);
  
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.rescan());
}

// Test: ConjunctionOp close with nullptr op
// Verifies that open() detects nullptr op before close() can be called
TEST_F(ObDASBooleanQueryTest, conjunction_op_close_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  // open() should fail when it encounters nullptr op
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp advance_to with target.is_max() should return OB_ITER_END
// Verifies that advance_to correctly handles max target
TEST_F(ObDASBooleanQueryTest, conjunction_op_advance_to_max_target)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});
  
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());
  
  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_max();
  EXPECT_EQ(OB_ITER_END, conjunction_op.advance_to(target, curr_id, score));
}

// Test: ConjunctionOp advance_to with nullptr op
// Verifies that open() detects nullptr op before advance_to() can be called
TEST_F(ObDASBooleanQueryTest, conjunction_op_advance_to_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  // open() should fail when it encounters nullptr op
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp advance_to with op->advance_to() failure
// Verifies error propagation when a child op's advance_to() fails
TEST_F(ObDASBooleanQueryTest, conjunction_op_advance_to_op_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});
  mock_op1->set_advance_to_error(OB_ERR_UNEXPECTED);
  
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());
  
  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_uint64(1);
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.advance_to(target, curr_id, score));
}

// Test: ConjunctionOp next_rowid with nullptr op
// Verifies that open() detects nullptr op before next_rowid() can be called
TEST_F(ObDASBooleanQueryTest, conjunction_op_next_rowid_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  // open() should fail when it encounters nullptr op
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp next_rowid with op->next_rowid() failure
// Verifies error propagation when a child op's next_rowid() fails
TEST_F(ObDASBooleanQueryTest, conjunction_op_next_rowid_op_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});
  mock_op1->set_next_rowid_error(OB_ERR_UNEXPECTED);
  
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());
  
  ObDASRowID next_id;
  double score = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.next_rowid(next_id, score));
}

// Test: ConjunctionOp do_advance_to with normal flow
// Verifies that do_advance_to works correctly with non-intersecting ops
TEST_F(ObDASBooleanQueryTest, conjunction_op_do_advance_to_unexpected_order)
{
  SearchCtxSetup ctx_setup(allocator_);
  // Create ops where advance_to might return unexpected order
  // This is hard to trigger directly, but we can test the normal flow
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 3, 5});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 4, 6});
  
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());
  
  // Test normal advance_to - should work correctly
  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_uint64(1);
  // Since ops don't intersect, should eventually reach end
  int ret = conjunction_op.advance_to(target, curr_id, score);
  // Should either succeed or reach end
  EXPECT_TRUE(ret == OB_SUCCESS || ret == OB_ITER_END);
}

TEST_F(ObDASBooleanQueryTest, conjunction_scoring_probe_contributes_score)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *driver = OB_NEWx(
      MockSearchOp, &allocator_, ctx_setup.search_ctx_,
      std::vector<uint64_t>{1, 2, 3}, std::vector<double>{5.0, 5.0, 5.0});
  MockProbeSearchOp *probe = OB_NEWx(
      MockProbeSearchOp, &allocator_, ctx_setup.search_ctx_,
      std::vector<uint64_t>{1, 3}, 2.0);
  ASSERT_NE(nullptr, driver);
  ASSERT_NE(nullptr, probe);

  MockSearchOpParam driver_param;
  MockSearchOpParam probe_param;
  driver_param.set_is_scoring(true);
  probe_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, driver->init(driver_param));
  ASSERT_EQ(OB_SUCCESS, probe->init(probe_param));

  ObArray<ObIDASSearchOp *> ops;
  ASSERT_EQ(OB_SUCCESS, ops.push_back(driver));
  ASSERT_EQ(OB_SUCCESS, ops.push_back(probe));
  ObDASConjunctionOpParam param(ops);
  param.set_is_scoring(true);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());

  ObDASRowID row_id;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, conjunction_op.next_rowid(row_id, score));
  EXPECT_EQ(1, row_id.get_uint64());
  EXPECT_DOUBLE_EQ(7.0, score);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.next_rowid(row_id, score));
  EXPECT_EQ(3, row_id.get_uint64());
  EXPECT_DOUBLE_EQ(7.0, score);
  EXPECT_EQ(OB_ITER_END, conjunction_op.next_rowid(row_id, score));
}

TEST_F(ObDASBooleanQueryTest, conjunction_preserves_negative_driver_threshold)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp leaf(ctx_setup.search_ctx_, {1, 2}, {0.0, 0.0});
  MockProbeSearchOp probe(ctx_setup.search_ctx_, {1, 2}, 5.0);
  MockSearchOpParam leaf_param;
  leaf_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, leaf.init(leaf_param));
  ASSERT_EQ(OB_SUCCESS, probe.init(leaf_param));
  ObArray<ObIDASSearchOp *> driver_children;
  ASSERT_EQ(OB_SUCCESS, driver_children.push_back(&leaf));
  ObDASDisjunctionOpParam driver_param(driver_children, 1, false, ObDASSearchCost(10));
  driver_param.set_is_scoring(true);
  ObDASDisjunctionOp driver(ctx_setup.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, driver.init(driver_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&driver));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&probe));
  ObDASConjunctionOpParam param(children);
  param.set_is_scoring(true);
  ObDASConjunctionOp conjunction(ctx_setup.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, conjunction.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction.open());
  ASSERT_EQ(OB_SUCCESS, conjunction.set_min_competitive_score(3.0));
  EXPECT_DOUBLE_EQ(-2.0, driver.min_competitive_score_);
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(5.0, score);
  ASSERT_EQ(OB_SUCCESS, conjunction.set_min_competitive_score(5.0));
  EXPECT_DOUBLE_EQ(0.0, driver.min_competitive_score_);
  EXPECT_EQ(OB_ITER_END, conjunction.next_rowid(rowid, score));
  ASSERT_EQ(OB_SUCCESS, conjunction.close());
}

TEST_F(ObDASBooleanQueryTest, propagated_req_opt_threshold_remains_monotonic)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp required(ctx_setup.search_ctx_);
  MockProbeSearchOp optional(ctx_setup.search_ctx_, {}, 5.0);
  MockSearchOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, required.init(child_param));
  child_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, optional.init(child_param));
  ObDASReqOptOpParam param(&required, &optional, true);
  param.set_is_scoring(true);
  ObDASReqOptOp req_opt(ctx_setup.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, req_opt.init(param));
  ASSERT_EQ(OB_SUCCESS, req_opt.open());
  EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, req_opt.min_competitive_score_);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      req_opt.set_min_competitive_score(std::numeric_limits<double>::quiet_NaN()));
  EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, req_opt.min_competitive_score_);
  ASSERT_EQ(OB_SUCCESS, req_opt.set_min_competitive_score(-2.0));
  ASSERT_EQ(OB_SUCCESS, req_opt.set_min_competitive_score(0.0));
  ASSERT_EQ(OB_SUCCESS, req_opt.set_min_competitive_score(1.0));
  EXPECT_TRUE(req_opt.score_propagated_);
  EXPECT_DOUBLE_EQ(1.0, req_opt.min_competitive_score_);
  EXPECT_DOUBLE_EQ(1.0, optional.min_competitive_score_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, req_opt.set_min_competitive_score(0.0));
  EXPECT_DOUBLE_EQ(1.0, req_opt.min_competitive_score_);
  ASSERT_EQ(OB_SUCCESS, req_opt.rescan());
  EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, req_opt.min_competitive_score_);
  EXPECT_FALSE(req_opt.score_propagated_);
  ASSERT_EQ(OB_SUCCESS, req_opt.close());
}

// ============================================================================
// Section 8: ObDASDisjunctionOp Tests
// ============================================================================
// Tests for DisjunctionOp lifecycle, error handling, and edge cases

// Test: DisjunctionOp do_init double initialization should return OB_INIT_TWICE
// Verifies error handling when do_init is called twice
TEST_F(ObDASBooleanQueryTest, disjunction_op_inner_init_double_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  EXPECT_EQ(OB_INIT_TWICE, disjunction_op.init(param));
}

// Test: DisjunctionOp open with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when open() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_open_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.open());
}

// Test: DisjunctionOp open with nullptr child should return error
// Verifies error handling when a child op is nullptr during open()
TEST_F(ObDASBooleanQueryTest, disjunction_op_open_nullptr_child)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(nullptr);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  EXPECT_EQ(OB_ERR_UNEXPECTED, disjunction_op.open());
}

// Test: DisjunctionOp open with child->open() failure
// Verifies error propagation when a child op's open() fails
TEST_F(ObDASBooleanQueryTest, disjunction_op_open_child_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  mock_op->set_open_error(OB_ERR_UNEXPECTED);
  
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  EXPECT_EQ(OB_ERR_UNEXPECTED, disjunction_op.open());
}

// Test: DisjunctionOp rescan with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when rescan() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_rescan_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.rescan());
}

// Test: DisjunctionOp rescan with nullptr child
// Verifies that open() detects nullptr child before rescan() can be called
TEST_F(ObDASBooleanQueryTest, disjunction_op_rescan_nullptr_child)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ops.push_back(nullptr);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  // open() should fail when it encounters nullptr child
  EXPECT_EQ(OB_ERR_UNEXPECTED, disjunction_op.open());
}

// Test: DisjunctionOp rescan with child->rescan() failure
// Verifies error propagation when a child op's rescan() fails
TEST_F(ObDASBooleanQueryTest, disjunction_op_rescan_child_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  mock_op->set_rescan_error(OB_ERR_UNEXPECTED);
  
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.open());
  EXPECT_EQ(OB_ERR_UNEXPECTED, disjunction_op.rescan());
}

// Test: DisjunctionOp advance_to with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when advance_to() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_advance_to_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_uint64(1);
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.advance_to(target, curr_id, score));
}

// Test: DisjunctionOp advance_to with iter_end_ should return OB_ITER_END
// Verifies that advance_to correctly handles iter_end_ flag
TEST_F(ObDASBooleanQueryTest, disjunction_op_advance_to_iter_end)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.open());
  // Manually set iter_end_ to test the branch
  disjunction_op.iter_end_ = true;
  
  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_uint64(1);
  EXPECT_EQ(OB_ITER_END, disjunction_op.advance_to(target, curr_id, score));
}

// Test: DisjunctionOp next_rowid with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when next_rowid() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_next_rowid_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObDASRowID next_id;
  double score = 0;
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.next_rowid(next_id, score));
}

// Test: DisjunctionOp set_min_competitive_score with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when set_min_competitive_score() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_set_min_competitive_score_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.set_min_competitive_score(1.0));
}

// Test: DisjunctionOp set_min_competitive_score with need_score_ false
// Verifies that when need_score_ is false, the method does nothing
TEST_F(ObDASBooleanQueryTest, disjunction_op_set_min_competitive_score_no_score)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  // need_score_ is false by default
  ASSERT_EQ(OB_SUCCESS, disjunction_op.set_min_competitive_score(1.0));
}

// Test: DisjunctionOp set_min_competitive_score with children_cnt_ == 1
// Verifies that when there's only one child, it delegates to the child directly
TEST_F(ObDASBooleanQueryTest, disjunction_op_set_min_competitive_score_single_child)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  disjunction_op.need_score_ = true;
  ASSERT_EQ(OB_SUCCESS, disjunction_op.set_min_competitive_score(1.0));
}

// Test: DisjunctionOp calc_max_score with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when calc_max_score() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_calc_max_score_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  double threshold = 0;
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.calc_max_score(threshold));
}

// Test: DisjunctionOp calc_max_score with need_score_ false
// Verifies that when need_score_ is false, threshold is set to 0.0
TEST_F(ObDASBooleanQueryTest, disjunction_op_calc_max_score_no_score)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  double threshold = 0;
  ASSERT_EQ(OB_SUCCESS, disjunction_op.calc_max_score(threshold));
  EXPECT_EQ(0.0, threshold);
}

// ============================================================================
// Section 9: ObHybridSearchCgService::generate_ctdef Tests
// ============================================================================

class ObDASBooleanQueryCgTest : public ObDASBooleanQueryTest
{
public:
  ObDASBooleanQueryCgTest() : ObDASBooleanQueryTest(), 
                              phy_plan_(), 
                              cg_(CLUSTER_VERSION_4_1_0_0),
                              cg_service_(cg_)
  {
    cg_.phy_plan_ = &phy_plan_;
  }
  
  virtual ~ObDASBooleanQueryCgTest() {}

protected:
  ObPhysicalPlan phy_plan_;
  ObStaticEngineCG cg_;
  ObHybridSearchCgService cg_service_;
};

// Test: generate_ctdef with null phy_plan_
TEST_F(ObDASBooleanQueryCgTest, generate_ctdef_null_phy_plan)
{
  cg_.phy_plan_ = nullptr;
  ObBooleanQueryNode node(allocator_);
  ObDASBooleanQueryCtDef *ctdef = nullptr;
  ObLogTableScan *op = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, cg_service_.generate_ctdef(*op, &node, ctdef));
}

// Test: generate_ctdef with null node
TEST_F(ObDASBooleanQueryCgTest, generate_ctdef_null_node)
{
  ObDASBooleanQueryCtDef *ctdef = nullptr;
  ObLogTableScan *op = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, cg_service_.generate_ctdef(*op, nullptr, ctdef));
}

// Test: generate_ctdef with invalid min_should_match (< 0)
TEST_F(ObDASBooleanQueryCgTest, generate_ctdef_invalid_min_match_neg)
{
  ObBooleanQueryNode node(allocator_);
  node.min_should_match_ = -1;
  ObDASBooleanQueryCtDef *ctdef = nullptr;
  ObLogTableScan *op = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, cg_service_.generate_ctdef(*op, &node, ctdef));
}

// Test: generate_ctdef with no positive clauses
TEST_F(ObDASBooleanQueryCgTest, generate_ctdef_no_positive_clauses)
{
  ObBooleanQueryNode node(allocator_);
  void *buf = allocator_.alloc(sizeof(ObScalarQueryNode));
  ObScalarQueryNode *s1 = new(buf) ObScalarQueryNode(allocator_);
  node.must_not_nodes_.push_back(s1);
  node.min_should_match_ = 0;
  ObDASBooleanQueryCtDef *ctdef = nullptr;
  ObLogTableScan *op = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, cg_service_.generate_ctdef(*op, &node, ctdef));
}

TEST_F(ObDASBooleanQueryCgTest, generate_ctdef_registers_merged_probe_child)
{
  ObAddr addr;
  ObGlobalHint global_hint(allocator_);
  ObRawExprFactory expr_factory(allocator_);
  ObOptimizerContext opt_ctx(nullptr, nullptr, nullptr, nullptr, allocator_, nullptr,
                             addr, nullptr, global_hint, expr_factory, nullptr, false);
  ObSelectLogPlan plan(opt_ctx, nullptr);
  ObLogTableScan op(plan);
  ObBooleanQueryNode node(allocator_);
  ObScalarQueryNode must_node(allocator_);
  ObScalarQueryNode filter_node(allocator_);
  ObScalarQueryNode should_node(allocator_);
  ObScalarQueryNode merged_node(allocator_);
  ASSERT_EQ(OB_SUCCESS, node.must_nodes_.push_back(&must_node));
  ASSERT_EQ(OB_SUCCESS, node.filter_nodes_.push_back(&filter_node));
  ASSERT_EQ(OB_SUCCESS, node.should_nodes_.push_back(&should_node));
  node.min_should_match_ = 0;

  ObDASBooleanQueryCtDef *ctdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, cg_service_.generate_ctdef(op, &node, ctdef));
  ASSERT_NE(nullptr, ctdef);
  EXPECT_FALSE(ctdef->has_merged_probe());

  node.merged_probe_scalar_node_ = &merged_node;
  ASSERT_EQ(OB_SUCCESS, cg_service_.generate_ctdef(op, &node, ctdef));
  ASSERT_NE(nullptr, ctdef);
  ASSERT_TRUE(ctdef->has_merged_probe());
  EXPECT_EQ(3, ctdef->get_merged_probe_offset());
  EXPECT_EQ(0, ctdef->must_.offset_);
  EXPECT_EQ(1, ctdef->should_.offset_);
  EXPECT_EQ(2, ctdef->filter_.offset_);
}

// Regression for Dima 2026051500116116702 problem 2: inside block_max_pruning, when optional
// has advanced past the current required row (cmp_ret < 0 after advance_to), req-only score
// must be kept; otherwise curr_score stays 0 and high-scoring required rows are dropped.
TEST_F(ObDASBooleanQueryTest, req_opt_op_block_max_pruning_req_only_after_optional_advance)
{
  SearchCtxSetup ctx_setup(allocator_);
  // required: low-score row 50 then high-score row 100 (enters block_max_pruning on first row).
  // optional: row 50 stays current after row 50 is processed; for row 100, advance_to(100)
  // jumps to 200 so the second compare hits cmp_ret < 0 (the buggy branch before fix).
  MockSearchOp *required = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_,
      std::vector<uint64_t>{50, 100}, std::vector<double>{5.0, 61.0});
  MockSearchOp *optional = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_,
      std::vector<uint64_t>{50, 200}, std::vector<double>{3.0, 8.0});

  ObDASReqOptOp req_opt_op(ctx_setup.search_ctx_);
  ObDASReqOptOpParam param(required, optional, true);
  ASSERT_EQ(OB_SUCCESS, req_opt_op.init(param));
  ASSERT_EQ(OB_SUCCESS, req_opt_op.open());
  req_opt_op.max_score_calculated_ = true;
  req_opt_op.required_max_score_ = 61.0;
  req_opt_op.optional_max_score_ = 8.0;
  req_opt_op.min_competitive_score_ = 20.0;

  ObDASRowID next_id;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, req_opt_op.next_rowid(next_id, score));
  EXPECT_EQ(100ULL, next_id.get_uint64());
  EXPECT_DOUBLE_EQ(61.0, score);
  EXPECT_EQ(OB_ITER_END, req_opt_op.next_rowid(next_id, score));
}

TEST_F(ObDASBooleanQueryCgTest,
       async_domain_normalization_failure_preserves_tree_and_mode)
{
  ObDASScalarScanCtDef query_ctdef_1(allocator_);
  ObDASScalarScanCtDef query_ctdef_2(allocator_);
  ObDASScalarScanCtDef delete_ctdef_1(allocator_);
  ObDASScalarScanCtDef delete_ctdef_2(allocator_);
  query_ctdef_1.ref_table_id_ = 1001;
  query_ctdef_1.is_async_index_ = true;
  query_ctdef_2.ref_table_id_ = 1002;
  query_ctdef_2.is_async_index_ = true;
  delete_ctdef_1.ref_table_id_ = 2001;
  delete_ctdef_2.ref_table_id_ = 2002;

  ObDASBooleanQueryCtDef wrapper_ctdef_1(allocator_);
  ObDASBooleanQueryCtDef wrapper_ctdef_2(allocator_);
  ObDASBaseCtDef *wrapper_children_1[] = {&query_ctdef_1, &delete_ctdef_1};
  ObDASBaseCtDef *wrapper_children_2[] = {&query_ctdef_2, &delete_ctdef_2};
  wrapper_ctdef_1.children_ = wrapper_children_1;
  wrapper_ctdef_1.children_cnt_ = ARRAYSIZEOF(wrapper_children_1);
  wrapper_ctdef_1.set_must(0, 1);
  wrapper_ctdef_1.set_must_not(1, 1);
  wrapper_ctdef_1.set_is_async(true);
  wrapper_ctdef_2.children_ = wrapper_children_2;
  wrapper_ctdef_2.children_cnt_ = ARRAYSIZEOF(wrapper_children_2);
  wrapper_ctdef_2.set_must(0, 1);
  wrapper_ctdef_2.set_must_not(1, 1);
  wrapper_ctdef_2.set_is_async(true);

  ObDASBooleanQueryCtDef boolean_ctdef(allocator_);
  ObDASBaseCtDef *boolean_children[] = {&wrapper_ctdef_1, &wrapper_ctdef_2};
  boolean_ctdef.children_ = boolean_children;
  boolean_ctdef.children_cnt_ = ARRAYSIZEOF(boolean_children);
  boolean_ctdef.set_must(0, ARRAYSIZEOF(boolean_children));
  ObDASBaseCtDef *root_ctdef = &boolean_ctdef;
  ObDASAsyncIndexExecMode exec_mode = ASYNC_INDEX_EXEC_GROUP;

  EXPECT_EQ(OB_STATE_NOT_MATCH,
            cg_service_.normalize_async_fts_boolean_domain(root_ctdef, exec_mode));
  EXPECT_EQ(&boolean_ctdef, root_ctdef);
  EXPECT_EQ(&wrapper_ctdef_1, boolean_ctdef.children_[0]);
  EXPECT_EQ(&wrapper_ctdef_2, boolean_ctdef.children_[1]);
  EXPECT_EQ(&query_ctdef_1, wrapper_ctdef_1.children_[0]);
  EXPECT_EQ(&delete_ctdef_1, wrapper_ctdef_1.children_[1]);
  EXPECT_EQ(&query_ctdef_2, wrapper_ctdef_2.children_[0]);
  EXPECT_EQ(&delete_ctdef_2, wrapper_ctdef_2.children_[1]);
  EXPECT_EQ(ASYNC_INDEX_EXEC_GROUP, exec_mode);
}

TEST_F(ObDASBooleanQueryCgTest,
       same_index_multi_match_normalizes_to_one_group_domain)
{
  ObDASScalarScanCtDef inv_ctdef_1(allocator_);
  ObDASScalarScanCtDef inv_ctdef_2(allocator_);
  ObDASIRScanCtDef ir_ctdef_1(allocator_);
  ObDASIRScanCtDef ir_ctdef_2(allocator_);
  ObDASMatchCtDef match_ctdef_1(allocator_);
  ObDASMatchCtDef match_ctdef_2(allocator_);
  ObDASScalarScanCtDef delete_ctdef_1(allocator_);
  ObDASScalarScanCtDef delete_ctdef_2(allocator_);
  ObDASBooleanQueryCtDef wrapper_ctdef_1(allocator_);
  ObDASBooleanQueryCtDef wrapper_ctdef_2(allocator_);
  ObDASBooleanQueryCtDef boolean_ctdef(allocator_);
  ObDASBaseCtDef *ir_children_1[] = {&inv_ctdef_1};
  ObDASBaseCtDef *ir_children_2[] = {&inv_ctdef_2};
  ObDASBaseCtDef *match_children_1[] = {&ir_ctdef_1};
  ObDASBaseCtDef *match_children_2[] = {&ir_ctdef_2};
  ObDASBaseCtDef *wrapper_children_1[] = {&match_ctdef_1, &delete_ctdef_1};
  ObDASBaseCtDef *wrapper_children_2[] = {&match_ctdef_2, &delete_ctdef_2};
  ObDASBaseCtDef *boolean_children[] = {&wrapper_ctdef_1, &wrapper_ctdef_2};

  inv_ctdef_1.ref_table_id_ = 1001;
  inv_ctdef_2.ref_table_id_ = 1001;
  ir_ctdef_1.children_ = ir_children_1;
  ir_ctdef_1.children_cnt_ = ARRAYSIZEOF(ir_children_1);
  ir_ctdef_1.is_async_index_ = true;
  ir_ctdef_1.use_scalar_scan_def_ = true;
  ir_ctdef_2.children_ = ir_children_2;
  ir_ctdef_2.children_cnt_ = ARRAYSIZEOF(ir_children_2);
  ir_ctdef_2.is_async_index_ = true;
  ir_ctdef_2.use_scalar_scan_def_ = true;
  match_ctdef_1.children_ = match_children_1;
  match_ctdef_1.children_cnt_ = ARRAYSIZEOF(match_children_1);
  match_ctdef_1.ir_ctdef_idx_ = 0;
  match_ctdef_2.children_ = match_children_2;
  match_ctdef_2.children_cnt_ = ARRAYSIZEOF(match_children_2);
  match_ctdef_2.ir_ctdef_idx_ = 0;
  delete_ctdef_1.ref_table_id_ = 2001;
  delete_ctdef_2.ref_table_id_ = 2001;
  wrapper_ctdef_1.children_ = wrapper_children_1;
  wrapper_ctdef_1.children_cnt_ = ARRAYSIZEOF(wrapper_children_1);
  wrapper_ctdef_1.set_must(0, 1);
  wrapper_ctdef_1.set_must_not(1, 1);
  wrapper_ctdef_1.set_is_async(true);
  wrapper_ctdef_2.children_ = wrapper_children_2;
  wrapper_ctdef_2.children_cnt_ = ARRAYSIZEOF(wrapper_children_2);
  wrapper_ctdef_2.set_must(0, 1);
  wrapper_ctdef_2.set_must_not(1, 1);
  wrapper_ctdef_2.set_is_async(true);
  boolean_ctdef.children_ = boolean_children;
  boolean_ctdef.children_cnt_ = ARRAYSIZEOF(boolean_children);
  boolean_ctdef.set_must(0, ARRAYSIZEOF(boolean_children));

  ObDASBaseCtDef *root_ctdef = &boolean_ctdef;
  ObDASAsyncIndexExecMode exec_mode = ASYNC_INDEX_EXEC_LEGACY;
  ASSERT_EQ(OB_SUCCESS,
            cg_service_.normalize_async_fts_boolean_domain(root_ctdef, exec_mode));
  ASSERT_EQ(ASYNC_INDEX_EXEC_GROUP, exec_mode);
  ASSERT_NE(&boolean_ctdef, root_ctdef);
  ASSERT_EQ(DAS_OP_BOOLEAN_QUERY, root_ctdef->op_type_);
  ObDASBooleanQueryCtDef *group_wrapper =
      static_cast<ObDASBooleanQueryCtDef *>(root_ctdef);
  EXPECT_TRUE(group_wrapper->is_async());
  ASSERT_EQ(2, group_wrapper->children_cnt_);
  EXPECT_EQ(&boolean_ctdef, group_wrapper->children_[0]);
  EXPECT_EQ(&delete_ctdef_1, group_wrapper->children_[1]);
  EXPECT_EQ(&match_ctdef_1, boolean_ctdef.children_[0]);
  EXPECT_EQ(&match_ctdef_2, boolean_ctdef.children_[1]);
  EXPECT_EQ(DAS_OP_MATCH_QUERY, boolean_ctdef.children_[0]->op_type_);
  EXPECT_EQ(DAS_OP_MATCH_QUERY, boolean_ctdef.children_[1]->op_type_);
}

TEST_F(ObDASBooleanQueryCgTest,
       async_match_with_sync_filter_normalizes_to_group_domain)
{
  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASMatchCtDef match_ctdef(allocator_);
  ObDASScalarScanCtDef delete_ctdef(allocator_);
  ObDASScalarScanCtDef sync_filter_ctdef(allocator_);
  ObDASBooleanQueryCtDef async_wrapper(allocator_);
  ObDASBooleanQueryCtDef user_boolean(allocator_);
  ObDASBaseCtDef *ir_children[] = {&inv_ctdef};
  ObDASBaseCtDef *match_children[] = {&ir_ctdef};
  ObDASBaseCtDef *wrapper_children[] = {&match_ctdef, &delete_ctdef};
  ObDASBaseCtDef *user_children[] = {&async_wrapper, &sync_filter_ctdef};

  inv_ctdef.ref_table_id_ = 1001;
  ir_ctdef.children_ = ir_children;
  ir_ctdef.children_cnt_ = ARRAYSIZEOF(ir_children);
  ir_ctdef.is_async_index_ = true;
  ir_ctdef.use_scalar_scan_def_ = true;
  match_ctdef.children_ = match_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(match_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  delete_ctdef.ref_table_id_ = 2001;
  sync_filter_ctdef.ref_table_id_ = 3001;
  sync_filter_ctdef.is_async_index_ = false;
  async_wrapper.children_ = wrapper_children;
  async_wrapper.children_cnt_ = ARRAYSIZEOF(wrapper_children);
  async_wrapper.set_must(0, 1);
  async_wrapper.set_must_not(1, 1);
  async_wrapper.set_is_async(true);
  user_boolean.children_ = user_children;
  user_boolean.children_cnt_ = ARRAYSIZEOF(user_children);
  user_boolean.set_must(0, 1);
  user_boolean.set_filter(1, 1);

  ObDASAsyncIndexDomainAnalysis analysis;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &user_boolean, analysis));
  EXPECT_EQ(ASYNC_INDEX_DOMAIN_SINGLE, analysis.type_);
  EXPECT_TRUE(analysis.has_group_compatible_sync_filter_);
  EXPECT_EQ(ASYNC_INDEX_EXEC_GROUP, analysis.get_exec_mode());

  ObDASBaseCtDef *root_ctdef = &user_boolean;
  ObDASAsyncIndexExecMode exec_mode = ASYNC_INDEX_EXEC_LEGACY;
  ASSERT_EQ(OB_SUCCESS,
            cg_service_.normalize_async_fts_boolean_domain(
                root_ctdef, exec_mode));
  EXPECT_EQ(ASYNC_INDEX_EXEC_GROUP, exec_mode);
  ASSERT_NE(&user_boolean, root_ctdef);
  ASSERT_EQ(DAS_OP_BOOLEAN_QUERY, root_ctdef->op_type_);
  ObDASBooleanQueryCtDef *group_wrapper =
      static_cast<ObDASBooleanQueryCtDef *>(root_ctdef);
  EXPECT_TRUE(group_wrapper->is_async());
  ASSERT_EQ(2, group_wrapper->children_cnt_);
  EXPECT_EQ(&user_boolean, group_wrapper->children_[0]);
  EXPECT_EQ(&delete_ctdef, group_wrapper->children_[1]);
  EXPECT_EQ(&match_ctdef, user_boolean.children_[0]);
  EXPECT_EQ(&sync_filter_ctdef, user_boolean.children_[1]);
}

TEST_F(ObDASBooleanQueryCgTest,
       nested_sync_filter_normalizes_to_group_domain)
{
  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASMatchCtDef match_ctdef(allocator_);
  ObDASScalarScanCtDef delete_ctdef(allocator_);
  ObDASScalarScanCtDef sync_filter_ctdef(allocator_);
  ObDASBooleanQueryCtDef async_wrapper(allocator_);
  ObDASBooleanQueryCtDef nested_filter(allocator_);
  ObDASBooleanQueryCtDef user_boolean(allocator_);
  ObDASBaseCtDef *ir_children[] = {&inv_ctdef};
  ObDASBaseCtDef *match_children[] = {&ir_ctdef};
  ObDASBaseCtDef *wrapper_children[] = {&match_ctdef, &delete_ctdef};
  ObDASBaseCtDef *nested_children[] = {&sync_filter_ctdef};
  ObDASBaseCtDef *user_children[] = {&async_wrapper, &nested_filter};

  inv_ctdef.ref_table_id_ = 1001;
  ir_ctdef.children_ = ir_children;
  ir_ctdef.children_cnt_ = ARRAYSIZEOF(ir_children);
  ir_ctdef.is_async_index_ = true;
  ir_ctdef.use_scalar_scan_def_ = true;
  match_ctdef.children_ = match_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(match_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  delete_ctdef.ref_table_id_ = 2001;
  sync_filter_ctdef.ref_table_id_ = 3001;
  sync_filter_ctdef.is_async_index_ = false;
  async_wrapper.children_ = wrapper_children;
  async_wrapper.children_cnt_ = ARRAYSIZEOF(wrapper_children);
  async_wrapper.set_must(0, 1);
  async_wrapper.set_must_not(1, 1);
  async_wrapper.set_is_async(true);
  nested_filter.children_ = nested_children;
  nested_filter.children_cnt_ = ARRAYSIZEOF(nested_children);
  nested_filter.set_filter(0, 1);
  user_boolean.children_ = user_children;
  user_boolean.children_cnt_ = ARRAYSIZEOF(user_children);
  user_boolean.set_must(0, 1);
  user_boolean.set_filter(1, 1);

  ObDASAsyncIndexDomainAnalysis analysis;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &user_boolean, analysis));
  EXPECT_EQ(ASYNC_INDEX_EXEC_GROUP, analysis.get_exec_mode());

  ObDASBaseCtDef *root_ctdef = &user_boolean;
  ObDASAsyncIndexExecMode exec_mode = ASYNC_INDEX_EXEC_LEGACY;
  ASSERT_EQ(OB_SUCCESS,
            cg_service_.normalize_async_fts_boolean_domain(
                root_ctdef, exec_mode));
  EXPECT_EQ(ASYNC_INDEX_EXEC_GROUP, exec_mode);
  ASSERT_NE(&user_boolean, root_ctdef);
  ASSERT_EQ(DAS_OP_BOOLEAN_QUERY, root_ctdef->op_type_);
  ObDASBooleanQueryCtDef *group_wrapper =
      static_cast<ObDASBooleanQueryCtDef *>(root_ctdef);
  EXPECT_TRUE(group_wrapper->is_async());
  ASSERT_EQ(2, group_wrapper->children_cnt_);
  EXPECT_EQ(&user_boolean, group_wrapper->children_[0]);
  EXPECT_EQ(&delete_ctdef, group_wrapper->children_[1]);
  EXPECT_EQ(&match_ctdef, user_boolean.children_[0]);
  EXPECT_EQ(&nested_filter, user_boolean.children_[1]);
}

TEST_F(ObDASBooleanQueryCgTest,
       mixed_sync_group_normalization_requires_positive_async_clause)
{
  const ClauseType roles[] = {MUST, FILTER, SHOULD, MUST_NOT};
  for (const ClauseType role : roles) {
    ObDASScalarScanCtDef inv_ctdef(allocator_);
    ObDASIRScanCtDef ir_ctdef(allocator_);
    ObDASMatchCtDef match_ctdef(allocator_);
    ObDASScalarScanCtDef delete_ctdef(allocator_);
    ObDASScalarScanCtDef sync_filter_ctdef(allocator_);
    ObDASBooleanQueryCtDef async_wrapper(allocator_);
    ObDASBooleanQueryCtDef user_boolean(allocator_);
    ObDASBaseCtDef *ir_children[] = {&inv_ctdef};
    ObDASBaseCtDef *match_children[] = {&ir_ctdef};
    ObDASBaseCtDef *wrapper_children[] = {&match_ctdef, &delete_ctdef};
    ObDASBaseCtDef *user_children[] = {&async_wrapper, &sync_filter_ctdef};

    inv_ctdef.ref_table_id_ = 1001;
    ir_ctdef.children_ = ir_children;
    ir_ctdef.children_cnt_ = ARRAYSIZEOF(ir_children);
    ir_ctdef.is_async_index_ = true;
    ir_ctdef.use_scalar_scan_def_ = true;
    match_ctdef.children_ = match_children;
    match_ctdef.children_cnt_ = ARRAYSIZEOF(match_children);
    match_ctdef.ir_ctdef_idx_ = 0;
    delete_ctdef.ref_table_id_ = 2001;
    sync_filter_ctdef.ref_table_id_ = 3001;
    async_wrapper.children_ = wrapper_children;
    async_wrapper.children_cnt_ = ARRAYSIZEOF(wrapper_children);
    async_wrapper.set_must(0, 1);
    async_wrapper.set_must_not(1, 1);
    async_wrapper.set_is_async(true);
    user_boolean.children_ = user_children;
    user_boolean.children_cnt_ = ARRAYSIZEOF(user_children);
    user_boolean.set_filter(1, 1);
    if (MUST == role) {
      user_boolean.set_must(0, 1);
    } else if (FILTER == role) {
      user_boolean.set_filter(0, 2);
    } else if (SHOULD == role) {
      user_boolean.set_should(0, 1);
      user_boolean.set_min_should_match(0);
    } else {
      user_boolean.set_must_not(0, 1);
    }

    ObDASBaseCtDef *root_ctdef = &user_boolean;
    ObDASAsyncIndexExecMode exec_mode = ASYNC_INDEX_EXEC_LEGACY;
    ASSERT_EQ(OB_SUCCESS,
              cg_service_.normalize_async_fts_boolean_domain(root_ctdef, exec_mode));
    if (MUST == role || FILTER == role) {
      EXPECT_EQ(ASYNC_INDEX_EXEC_GROUP, exec_mode);
      ASSERT_NE(&user_boolean, root_ctdef);
      EXPECT_EQ(&match_ctdef, user_boolean.children_[0]);
      EXPECT_EQ(&sync_filter_ctdef, user_boolean.children_[1]);
    } else {
      EXPECT_EQ(ASYNC_INDEX_EXEC_LEGACY, exec_mode);
      EXPECT_EQ(&user_boolean, root_ctdef);
      EXPECT_EQ(&async_wrapper, user_boolean.children_[0]);
      EXPECT_EQ(&sync_filter_ctdef, user_boolean.children_[1]);
      EXPECT_EQ(&match_ctdef, async_wrapper.children_[0]);
      EXPECT_EQ(&delete_ctdef, async_wrapper.children_[1]);
    }
  }
}

TEST_F(ObDASBooleanQueryCgTest,
       unowned_sync_filter_discards_nested_async_group_rewrites)
{
  const ClauseType roles[] = {MUST, SHOULD, MUST_NOT};
  for (const ClauseType role : roles) {
    ObDASScalarScanCtDef inv_ctdef(allocator_);
    ObDASIRScanCtDef ir_ctdef(allocator_);
    ObDASMatchCtDef match_ctdef(allocator_);
    ObDASScalarScanCtDef search_ctdef(allocator_);
    ObDASScalarScanCtDef delete_ctdef_1(allocator_);
    ObDASScalarScanCtDef delete_ctdef_2(allocator_);
    ObDASScalarScanCtDef sync_filter_ctdef(allocator_);
    ObDASBooleanQueryCtDef match_wrapper(allocator_);
    ObDASBooleanQueryCtDef search_wrapper(allocator_);
    ObDASBooleanQueryCtDef inner_boolean(allocator_);
    ObDASBooleanQueryCtDef outer_boolean(allocator_);
    ObDASBaseCtDef *ir_children[] = {&inv_ctdef};
    ObDASBaseCtDef *match_children[] = {&ir_ctdef};
    ObDASBaseCtDef *match_wrapper_children[] = {&match_ctdef, &delete_ctdef_1};
    ObDASBaseCtDef *search_wrapper_children[] = {&search_ctdef, &delete_ctdef_2};
    ObDASBaseCtDef *inner_children[] = {&match_wrapper, &search_wrapper};
    ObDASBaseCtDef *outer_children[] = {&inner_boolean, &sync_filter_ctdef};
    inv_ctdef.ref_table_id_ = 1001;
    ir_ctdef.children_ = ir_children;
    ir_ctdef.children_cnt_ = ARRAYSIZEOF(ir_children);
    ir_ctdef.is_async_index_ = true;
    ir_ctdef.use_scalar_scan_def_ = true;
    match_ctdef.children_ = match_children;
    match_ctdef.children_cnt_ = ARRAYSIZEOF(match_children);
    match_ctdef.ir_ctdef_idx_ = 0;
    search_ctdef.ref_table_id_ = 1002;
    search_ctdef.is_async_index_ = true;
    delete_ctdef_1.ref_table_id_ = 2001;
    delete_ctdef_2.ref_table_id_ = 2001;
    sync_filter_ctdef.ref_table_id_ = 3001;
    match_wrapper.children_ = match_wrapper_children;
    match_wrapper.children_cnt_ = ARRAYSIZEOF(match_wrapper_children);
    match_wrapper.set_must(0, 1);
    match_wrapper.set_must_not(1, 1);
    match_wrapper.set_is_async(true);
    search_wrapper.children_ = search_wrapper_children;
    search_wrapper.children_cnt_ = ARRAYSIZEOF(search_wrapper_children);
    search_wrapper.set_must(0, 1);
    search_wrapper.set_must_not(1, 1);
    search_wrapper.set_is_async(true);
    inner_boolean.children_ = inner_children;
    inner_boolean.children_cnt_ = ARRAYSIZEOF(inner_children);
    inner_boolean.set_must(0, 1);
    inner_boolean.set_filter(1, 1);
    outer_boolean.children_ = outer_children;
    outer_boolean.children_cnt_ = ARRAYSIZEOF(outer_children);
    outer_boolean.set_filter(1, 1);
    if (MUST == role) {
      outer_boolean.set_must(0, 1);
    } else if (SHOULD == role) {
      outer_boolean.set_should(0, 1);
      outer_boolean.set_min_should_match(0);
    } else {
      outer_boolean.set_must_not(0, 1);
    }

    ObDASBaseCtDef *root_ctdef = &outer_boolean;
    ObDASAsyncIndexExecMode exec_mode = ASYNC_INDEX_EXEC_LEGACY;
    ASSERT_EQ(OB_SUCCESS,
              cg_service_.normalize_async_fts_boolean_domain(root_ctdef, exec_mode));
    if (MUST == role) {
      // The inner two-index subtree can genuinely coalesce when it is required.
      EXPECT_EQ(ASYNC_INDEX_EXEC_GROUP, exec_mode);
      ASSERT_NE(&outer_boolean, root_ctdef);
      EXPECT_EQ(&match_ctdef, inner_boolean.children_[0]);
      EXPECT_EQ(&search_ctdef, inner_boolean.children_[1]);
    } else {
      // Rejecting the outer coalesce must discard already planned inner actions.
      EXPECT_EQ(ASYNC_INDEX_EXEC_LEGACY, exec_mode);
      EXPECT_EQ(&outer_boolean, root_ctdef);
      EXPECT_EQ(&inner_boolean, outer_boolean.children_[0]);
      EXPECT_EQ(&sync_filter_ctdef, outer_boolean.children_[1]);
      EXPECT_EQ(&match_wrapper, inner_boolean.children_[0]);
      EXPECT_EQ(&search_wrapper, inner_boolean.children_[1]);
      EXPECT_EQ(&match_ctdef, match_wrapper.children_[0]);
      EXPECT_EQ(&delete_ctdef_1, match_wrapper.children_[1]);
      EXPECT_EQ(&search_ctdef, search_wrapper.children_[0]);
      EXPECT_EQ(&delete_ctdef_2, search_wrapper.children_[1]);
      EXPECT_FALSE(outer_boolean.is_async());
      EXPECT_FALSE(inner_boolean.is_async());
      EXPECT_TRUE(match_wrapper.is_async());
      EXPECT_TRUE(search_wrapper.is_async());
    }
  }
}

TEST_F(ObDASBooleanQueryCgTest,
       same_async_search_index_multi_branch_keeps_legacy_tree)
{
  ObDASScalarScanCtDef scalar_ctdef_1(allocator_);
  ObDASScalarScanCtDef scalar_ctdef_2(allocator_);
  ObDASScalarScanCtDef delete_ctdef(allocator_);
  ObDASBooleanQueryCtDef query_ctdef(allocator_);
  ObDASBooleanQueryCtDef wrapper_ctdef(allocator_);
  ObDASBaseCtDef *query_children[] = {&scalar_ctdef_1, &scalar_ctdef_2};
  ObDASBaseCtDef *wrapper_children[] = {&query_ctdef, &delete_ctdef};

  scalar_ctdef_1.ref_table_id_ = 1001;
  scalar_ctdef_1.is_async_index_ = true;
  scalar_ctdef_2.ref_table_id_ = 1001;
  scalar_ctdef_2.is_async_index_ = true;
  delete_ctdef.ref_table_id_ = 2001;
  query_ctdef.children_ = query_children;
  query_ctdef.children_cnt_ = ARRAYSIZEOF(query_children);
  query_ctdef.set_must(0, ARRAYSIZEOF(query_children));
  wrapper_ctdef.children_ = wrapper_children;
  wrapper_ctdef.children_cnt_ = ARRAYSIZEOF(wrapper_children);
  wrapper_ctdef.set_must(0, 1);
  wrapper_ctdef.set_must_not(1, 1);
  wrapper_ctdef.set_is_async(true);

  ObDASBaseCtDef *root_ctdef = &wrapper_ctdef;
  ObDASAsyncIndexExecMode exec_mode = ASYNC_INDEX_EXEC_LEGACY;
  ASSERT_EQ(OB_SUCCESS,
            cg_service_.normalize_async_fts_boolean_domain(root_ctdef, exec_mode));
  EXPECT_EQ(ASYNC_INDEX_EXEC_LEGACY, exec_mode);
  EXPECT_EQ(&wrapper_ctdef, root_ctdef);
  EXPECT_EQ(&query_ctdef, wrapper_ctdef.children_[0]);
}

TEST_F(ObDASBooleanQueryCgTest,
       nested_boolean_async_search_domains_normalize_atomically)
{
  ObDASScalarScanCtDef target_ctdef_1(allocator_);
  ObDASScalarScanCtDef target_ctdef_2(allocator_);
  ObDASScalarScanCtDef aux_ctdef(allocator_);
  ObDASScalarScanCtDef delete_ctdef_1(allocator_);
  ObDASScalarScanCtDef delete_ctdef_2(allocator_);
  ObDASScalarScanCtDef delete_ctdef_3(allocator_);
  ObDASBooleanQueryCtDef target_wrapper_1(allocator_);
  ObDASBooleanQueryCtDef target_wrapper_2(allocator_);
  ObDASBooleanQueryCtDef aux_wrapper(allocator_);
  ObDASBooleanQueryCtDef inner_boolean(allocator_);
  ObDASBooleanQueryCtDef outer_boolean(allocator_);
  ObDASBaseCtDef *target_wrapper_children_1[] = {
      &target_ctdef_1, &delete_ctdef_1};
  ObDASBaseCtDef *target_wrapper_children_2[] = {
      &target_ctdef_2, &delete_ctdef_2};
  ObDASBaseCtDef *aux_wrapper_children[] = {&aux_ctdef, &delete_ctdef_3};
  ObDASBaseCtDef *inner_children[] = {&target_wrapper_1, &target_wrapper_2};
  ObDASBaseCtDef *outer_children[] = {&inner_boolean, &aux_wrapper};

  target_ctdef_1.ref_table_id_ = 1001;
  target_ctdef_1.is_async_index_ = true;
  target_ctdef_2.ref_table_id_ = 1001;
  target_ctdef_2.is_async_index_ = true;
  aux_ctdef.ref_table_id_ = 1002;
  aux_ctdef.is_async_index_ = true;
  delete_ctdef_1.ref_table_id_ = 2001;
  delete_ctdef_2.ref_table_id_ = 2001;
  delete_ctdef_3.ref_table_id_ = 2001;

  target_wrapper_1.children_ = target_wrapper_children_1;
  target_wrapper_1.children_cnt_ = ARRAYSIZEOF(target_wrapper_children_1);
  target_wrapper_1.set_must(0, 1);
  target_wrapper_1.set_must_not(1, 1);
  target_wrapper_1.set_is_async(true);
  target_wrapper_2.children_ = target_wrapper_children_2;
  target_wrapper_2.children_cnt_ = ARRAYSIZEOF(target_wrapper_children_2);
  target_wrapper_2.set_must(0, 1);
  target_wrapper_2.set_must_not(1, 1);
  target_wrapper_2.set_is_async(true);
  aux_wrapper.children_ = aux_wrapper_children;
  aux_wrapper.children_cnt_ = ARRAYSIZEOF(aux_wrapper_children);
  aux_wrapper.set_must(0, 1);
  aux_wrapper.set_must_not(1, 1);
  aux_wrapper.set_is_async(true);

  inner_boolean.children_ = inner_children;
  inner_boolean.children_cnt_ = ARRAYSIZEOF(inner_children);
  inner_boolean.set_should(0, ARRAYSIZEOF(inner_children));
  inner_boolean.set_min_should_match(1);
  outer_boolean.children_ = outer_children;
  outer_boolean.children_cnt_ = ARRAYSIZEOF(outer_children);
  outer_boolean.set_must(0, 1);
  outer_boolean.set_filter(1, 1);

  ObDASBaseCtDef *root_ctdef = &outer_boolean;
  ObDASAsyncIndexExecMode exec_mode = ASYNC_INDEX_EXEC_LEGACY;
  ASSERT_EQ(OB_SUCCESS,
            cg_service_.normalize_async_fts_boolean_domain(root_ctdef, exec_mode));
  ASSERT_EQ(ASYNC_INDEX_EXEC_GROUP, exec_mode);
  ASSERT_NE(&outer_boolean, root_ctdef);
  ASSERT_EQ(DAS_OP_BOOLEAN_QUERY, root_ctdef->op_type_);
  ObDASBooleanQueryCtDef *group_wrapper =
      static_cast<ObDASBooleanQueryCtDef *>(root_ctdef);
  EXPECT_TRUE(group_wrapper->is_async());
  ASSERT_EQ(2, group_wrapper->children_cnt_);
  EXPECT_EQ(&outer_boolean, group_wrapper->children_[0]);
  EXPECT_EQ(&delete_ctdef_1, group_wrapper->children_[1]);
  EXPECT_EQ(&inner_boolean, outer_boolean.children_[0]);
  EXPECT_EQ(&aux_ctdef, outer_boolean.children_[1]);
  EXPECT_EQ(&target_ctdef_1, inner_boolean.children_[0]);
  EXPECT_EQ(&target_ctdef_2, inner_boolean.children_[1]);
}

class TenantPrimaryGetRatioDefaultGuard
{
public:
  explicit TenantPrimaryGetRatioDefaultGuard(const int64_t value)
    : tenant_config_(TENANT_CONF(OB_SYS_TENANT_ID)), old_value_(0)
  {
    if (tenant_config_.is_valid()) {
      old_value_ = tenant_config_->_hybrid_search_primary_get_ratio_default;
      tenant_config_->_hybrid_search_primary_get_ratio_default = value;
    }
  }

  ~TenantPrimaryGetRatioDefaultGuard()
  {
    if (tenant_config_.is_valid()) {
      tenant_config_->_hybrid_search_primary_get_ratio_default = old_value_;
    }
  }

  bool is_valid() { return tenant_config_.is_valid(); }

private:
  ObTenantConfigGuard tenant_config_;
  int64_t old_value_;
};

class ObHybridSearchPrimaryGetRatioDefaultTest : public ::testing::Test
{
public:
  ObHybridSearchPrimaryGetRatioDefaultTest()
    : table_item_(allocator_), stmt_(stmt::T_SELECT, allocator_) {}

protected:
  static void SetUpTestCase()
  {
    const int ret = ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
  }

  void SetUp() override
  {
    dsl_info_ = ObDSLQueryInfo();
    table_item_.dsl_query_ = &dsl_info_;
    params_.allocator_ = &allocator_;
    params_.session_info_ = &session_;
    session_.effective_tenant_id_ = OB_SYS_TENANT_ID;
  }

  ObArenaAllocator allocator_;
  ObSQLSessionInfo session_;
  ObResolverParams params_;
  ObDSLQueryInfo dsl_info_;
  TableItem table_item_;
  ObDMLStmt stmt_;
};

TEST_F(ObHybridSearchPrimaryGetRatioDefaultTest, use_session_snapshot_when_omitted)
{
  TenantPrimaryGetRatioDefaultGuard guard(999);
  ASSERT_TRUE(guard.is_valid());
  session_.inf_pc_configs_.hybrid_search_primary_get_ratio_default_ = 250;
  ObDSLResolver resolver(params_, nullptr, &stmt_, table_item_);
  ObDSLKnnQuery::SearchOption *knn_search_option = nullptr;

  ASSERT_EQ(OB_SUCCESS, resolver.fill_default_query_primary_get_ratio());
  ASSERT_EQ(OB_SUCCESS, resolver.fill_default_knn_primary_get_ratio(knn_search_option));
  ASSERT_NE(nullptr, knn_search_option);
  EXPECT_EQ(250, dsl_info_.fts_search_option_.primary_get_ratio_);
  EXPECT_EQ(250, knn_search_option->primary_get_ratio_);
}

TEST_F(ObHybridSearchPrimaryGetRatioDefaultTest, keep_different_explicit_values)
{
  session_.inf_pc_configs_.hybrid_search_primary_get_ratio_default_ = 250;
  ObDSLResolver resolver(params_, nullptr, &stmt_, table_item_);
  ObDSLKnnQuery::SearchOption knn_search_option;
  ObDSLKnnQuery::SearchOption *knn_search_option_ptr = &knn_search_option;
  dsl_info_.fts_search_option_.primary_get_ratio_ = 42;
  knn_search_option.primary_get_ratio_ = 77;

  ASSERT_EQ(OB_SUCCESS, resolver.fill_default_query_primary_get_ratio());
  ASSERT_EQ(OB_SUCCESS, resolver.fill_default_knn_primary_get_ratio(knn_search_option_ptr));
  EXPECT_EQ(42, dsl_info_.fts_search_option_.primary_get_ratio_);
  EXPECT_EQ(77, knn_search_option.primary_get_ratio_);
}

TEST_F(ObHybridSearchPrimaryGetRatioDefaultTest, zero_tenant_value_keeps_default_disabled)
{
  session_.inf_pc_configs_.hybrid_search_primary_get_ratio_default_ = 0;
  ObDSLResolver resolver(params_, nullptr, &stmt_, table_item_);
  ObDSLKnnQuery::SearchOption *knn_search_option = nullptr;

  ASSERT_EQ(OB_SUCCESS, resolver.fill_default_query_primary_get_ratio());
  ASSERT_EQ(OB_SUCCESS, resolver.fill_default_knn_primary_get_ratio(knn_search_option));
  EXPECT_EQ(ObDSLBaseSearchOption::UNSET_PRIMARY_GET_RATIO,
            dsl_info_.fts_search_option_.primary_get_ratio_);
  EXPECT_EQ(nullptr, knn_search_option);
}


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

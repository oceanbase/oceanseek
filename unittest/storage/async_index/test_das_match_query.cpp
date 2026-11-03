/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_match_phrase_query.h"
#include "sql/das/search/ob_das_multi_match_query.h"
#include "sql/das/search/ob_das_query_string_query.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/das/search/ob_das_bmm_op.h"
#include "sql/das/search/ob_das_conjunction_op.h"
#include "sql/das/search/ob_das_disjunction_op.h"
#include "sql/das/search/ob_das_dummy_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_async_segment_merge_op.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_das_fts_cost_utils.h"
#include "sql/das/search/ob_das_msm_runtime_util.h"
#include "storage/retrieval/ob_inv_idx_param_estimator.h"
#include "plugin/legacy/sys/ob_plugin_mgr.h"
#include "lib/container/ob_heap.h"
#undef private
#undef protected

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace
{

class ObDASMatchQueryTest : public ::testing::Test
{
protected:
  ObDASMatchQueryTest()
      : allocator_("TestDASMatch"),
        exec_ctx_(allocator_),
        eval_ctx_(exec_ctx_),
        ir_scan_ctdef_(allocator_),
        ir_scan_rtdef_(),
        inv_idx_scan_ctdef_(allocator_),
        inv_idx_scan_rtdef_(),
        match_ctdef_(allocator_),
        match_rtdef_(),
        match_phrase_ctdef_(allocator_),
        match_phrase_rtdef_(),
        multi_match_ctdef_(allocator_),
        multi_match_rtdef_(),
        query_string_ctdef_(allocator_),
        query_string_rtdef_()
  {
    exec_ctx_.frames_ = static_cast<char **>(allocator_.alloc(sizeof(char *)));
    exec_ctx_.frames_[0] = static_cast<char *>(allocator_.alloc(4096));
    MEMSET(exec_ctx_.frames_[0], 0, 4096);
    exec_ctx_.frame_cnt_ = 1;
    eval_ctx_.frames_ = exec_ctx_.frames_;
    match_rtdef_.ctdef_ = &match_ctdef_;
    match_rtdef_.eval_ctx_ = &eval_ctx_;
    match_phrase_rtdef_.ctdef_ = &match_phrase_ctdef_;
    match_phrase_rtdef_.eval_ctx_ = &eval_ctx_;
    multi_match_rtdef_.ctdef_ = &multi_match_ctdef_;
    multi_match_rtdef_.eval_ctx_ = &eval_ctx_;
    query_string_rtdef_.ctdef_ = &query_string_ctdef_;
    query_string_rtdef_.eval_ctx_ = &eval_ctx_;
    ir_scan_rtdef_.ctdef_ = &ir_scan_ctdef_;
    ir_scan_ctdef_.use_scalar_scan_def_ = true;
    inv_idx_scan_ctdef_.table_param_.parser_name_ = ObString::make_string("space.1");
    inv_idx_scan_ctdef_.table_param_.parser_properties_ = ObString::make_string(
        "{\"min_token_size\":1,\"max_token_size\":84,\"stopword_table\":\"none\","
        "\"dict_table\":\"none\",\"quanitfier_table\":\"none\",\"ngram_token_size\":2}");
    inv_idx_scan_ctdef_.table_param_.fts_index_type_ = share::schema::OB_FTS_INDEX_TYPE_MATCH;
    
    // Wire up children for Match CtDef/RtDef
    // Child 0: ObDASIRScanCtDef
    void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
    void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
    match_ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
    match_ctdef_.children_cnt_ = 1;
    match_ctdef_.children_[0] = &ir_scan_ctdef_;
    match_ctdef_.ir_ctdef_idx_ = 0;

    match_phrase_ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
    match_phrase_ctdef_.children_cnt_ = 1;
    match_phrase_ctdef_.children_[0] = &ir_scan_ctdef_;
    match_phrase_ctdef_.ir_ctdef_idx_ = 0;

    multi_match_ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
    multi_match_ctdef_.children_cnt_ = 1;
    multi_match_ctdef_.children_[0] = &ir_scan_ctdef_;
    EXPECT_EQ(OB_SUCCESS, multi_match_ctdef_.ir_ctdef_indices_.init(1));
    EXPECT_EQ(OB_SUCCESS, multi_match_ctdef_.ir_ctdef_indices_.prepare_allocate(1));
    multi_match_ctdef_.ir_ctdef_indices_.at(0) = 0;

    query_string_ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
    query_string_ctdef_.children_cnt_ = 1;
    query_string_ctdef_.children_[0] = &ir_scan_ctdef_;
    EXPECT_EQ(OB_SUCCESS, query_string_ctdef_.ir_ctdef_indices_.init(1));
    EXPECT_EQ(OB_SUCCESS, query_string_ctdef_.ir_ctdef_indices_.prepare_allocate(1));
    query_string_ctdef_.ir_ctdef_indices_.at(0) = 0;

    match_rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
    match_rtdef_.children_cnt_ = 1;
    match_rtdef_.children_[0] = &ir_scan_rtdef_;
    match_phrase_rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
    match_phrase_rtdef_.children_cnt_ = 1;
    match_phrase_rtdef_.children_[0] = &ir_scan_rtdef_;
    multi_match_rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
    multi_match_rtdef_.children_cnt_ = 1;
    multi_match_rtdef_.children_[0] = &ir_scan_rtdef_;
    query_string_rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
    query_string_rtdef_.children_cnt_ = 1;
    query_string_rtdef_.children_[0] = &ir_scan_rtdef_;

    // Wire up inv_idx_scan for IRScanCtDef
    void *ir_ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
    ir_scan_ctdef_.children_ = (ObDASBaseCtDef**)ir_ct_buf;
    ir_scan_ctdef_.children_cnt_ = 1;
    ir_scan_ctdef_.children_[0] = &inv_idx_scan_ctdef_;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
    setup_exprs();
  }

  void TearDown() override
  {
  }

  void setup_exprs()
  {
    // Alloc ObExpr for query_text, boost, match_operator, minimum_should_match
    match_ctdef_.query_text_ = alloc_expr(OB_MOCK_EXPR_FRAME_IDX, 0);
    match_ctdef_.boost_ = alloc_expr(OB_MOCK_EXPR_FRAME_IDX, 1);
    match_ctdef_.match_operator_ = alloc_expr(OB_MOCK_EXPR_FRAME_IDX, 2);
    match_ctdef_.minimum_should_match_ = alloc_expr(OB_MOCK_EXPR_FRAME_IDX, 3);

    match_ctdef_.query_text_->obj_meta_.set_varchar();
    match_ctdef_.query_text_->obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

    match_phrase_ctdef_.query_text_ = match_ctdef_.query_text_;
    match_phrase_ctdef_.boost_ = match_ctdef_.boost_;
    match_phrase_ctdef_.slop_ = alloc_expr(OB_MOCK_EXPR_FRAME_IDX, 4);

    multi_match_ctdef_.query_text_ = match_ctdef_.query_text_;
    multi_match_ctdef_.boost_ = match_ctdef_.boost_;
    multi_match_ctdef_.match_operator_ = match_ctdef_.match_operator_;
    multi_match_ctdef_.minimum_should_match_ = match_ctdef_.minimum_should_match_;
    query_string_ctdef_.query_text_ = match_ctdef_.query_text_;
    query_string_ctdef_.boost_ = match_ctdef_.boost_;
    query_string_ctdef_.default_operator_ = match_ctdef_.match_operator_;
    query_string_ctdef_.minimum_should_match_ = match_ctdef_.minimum_should_match_;
    query_string_ctdef_.query_text_->obj_meta_.set_varchar();
    query_string_ctdef_.query_text_->obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    query_string_ctdef_.query_text_->datum_meta_.type_ = ObVarcharType;
    query_string_ctdef_.query_text_->datum_meta_.cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;

    set_query_text("ocean base database");
    set_boost(1.0);
    set_slop(0);
    set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
    set_minimum_should_match(1);
  }

  ObExpr *alloc_expr(uint32_t frame_idx, uint32_t datum_off)
  {
    ObExpr *expr = (ObExpr *)allocator_.alloc(sizeof(ObExpr));
    new (expr) ObExpr();
    expr->frame_idx_ = frame_idx;
    expr->datum_off_ = datum_off * sizeof(ObDatum);
    expr->eval_info_off_ = 256 + datum_off * sizeof(ObEvalInfo);
    expr->res_buf_off_ = 512 + datum_off * 128;
    expr->res_buf_len_ = 128;
    expr->eval_func_ = nullptr;
    return expr;
  }

  void set_query_text(const char *str)
  {
    ObDatum &datum = match_ctdef_.query_text_->locate_datum_for_write(eval_ctx_);
    if (str == nullptr) {
      datum.set_null();
    } else {
      datum.set_string(ObString::make_string(str));
    }
  }

  void set_boost(double boost)
  {
    ObDatum &datum = match_ctdef_.boost_->locate_datum_for_write(eval_ctx_);
    datum.set_double(boost);
  }

  void set_slop(int64_t slop)
  {
    ObDatum &datum = match_phrase_ctdef_.slop_->locate_datum_for_write(eval_ctx_);
    datum.set_int(slop);
  }

  void set_match_operator(int64_t op)
  {
    ObDatum &datum = match_ctdef_.match_operator_->locate_datum_for_write(eval_ctx_);
    datum.set_int(op);
  }

  void set_minimum_should_match(int64_t msm)
  {
    ObDatum &datum = match_ctdef_.minimum_should_match_->locate_datum_for_write(eval_ctx_);
    datum.set_int(msm);
  }

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
      // 无 observer/MTL AccessService 时 init 会失败，本 UT 仅覆盖不依赖完整 storage 的路径
      // （cost / prepare_query_tokens / DummyOp），失败可忽略。
      int ret = search_ctx_.init(rowid_exprs, output, true);
      if (OB_SUCCESS != ret) {
        LOG_WARN("search ctx init skipped in storage-less unittest", K(ret));
      }
      search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    }

    void add_related_scan(
        const ObDASScalarScanCtDef &ctdef,
        ObDASScalarScanRtDef &rtdef,
        const ObTabletID &tablet_id)
    {
      mock_scan_op_.get_related_ctdefs().set_capacity(1);
      mock_scan_op_.get_related_rtdefs().set_capacity(1);
      mock_scan_op_.get_related_tablet_ids().set_capacity(1);
      ASSERT_EQ(OB_SUCCESS, mock_scan_op_.get_related_ctdefs().push_back(&ctdef));
      ASSERT_EQ(OB_SUCCESS, mock_scan_op_.get_related_rtdefs().push_back(&rtdef));
      ASSERT_EQ(OB_SUCCESS, mock_scan_op_.get_related_tablet_ids().push_back(tablet_id));
    }
  };

  static int add_mock_storage_group(ObAsyncIndexPartitionSegCtx &partition)
  {
    int ret = OB_SUCCESS;
    typedef storage::ObAsyncIndexSegmentGroup StorageSegmentGroup;
    StorageSegmentGroup *group = OB_NEWx(
        StorageSegmentGroup, &partition.storage_group_ctx_.allocator_);
    if (OB_ISNULL(group)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(group->init(1))) {
    } else {
      ObNewVersionRange range;
      range.base_version_ = 0;
      range.snapshot_version_ = 100;
      if (OB_FAIL(group->add_version_range(0, range))) {
      } else if (OB_FAIL(partition.storage_group_ctx_.groups_.push_back(group))) {
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(group)) {
      group->reset();
      OB_DELETEx(StorageSegmentGroup, &partition.storage_group_ctx_.allocator_, group);
    }
    return ret;
  }

  static int fetch_single_segment_partition(
      void *arg,
      const ObLSID &ls_id,
      const ObIArray<uint64_t> &index_table_ids,
      const ObIArray<ObTabletID> &index_tablet_ids,
      const ObTabletID &delete_tablet_id,
      const SCN &sql_read_snapshot,
      ObAsyncIndexPartitionSegCtx &partition)
  {
    int ret = OB_SUCCESS;
    UNUSED(arg);
    UNUSED(ls_id);
    UNUSED(delete_tablet_id);
    UNUSED(sql_read_snapshot);
    if (index_table_ids.count() != 1 || index_tablet_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(
                   partition.storage_group_ctx_.async_index_read_snapshot_.convert_for_tx(10000))) {
    } else if (OB_ISNULL(
                   partition.storage_group_ctx_.index_tablet_handles_.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ret = add_mock_storage_group(partition);
    }
    return ret;
  }

  void build_single_segment_catalog(
      ObDASAsyncIndexQueryCtx &catalog,
      ObDASSearchCtx &search_ctx,
      const uint64_t index_table_id)
  {
    ObSEArray<uint64_t, 1> index_tables;
    ObSEArray<ObTabletID, 1> index_tablets;
    SCN sql_snapshot;
    const ObAsyncIndexPartitionSegCtx *partition = nullptr;
    catalog.set_mock_fetcher(fetch_single_segment_partition, this);
    ASSERT_EQ(OB_SUCCESS, sql_snapshot.convert_for_tx(2000));
    ASSERT_EQ(OB_SUCCESS, index_tables.push_back(index_table_id));
    ASSERT_EQ(OB_SUCCESS, index_tablets.push_back(ObTabletID(10001)));
    ASSERT_EQ(OB_SUCCESS, catalog.add_partition(
        ObLSID(1001), ObTabletID(1), index_tables, index_tablets,
        ObTabletID(20001), sql_snapshot));
    ASSERT_EQ(OB_SUCCESS, catalog.finalize());
    ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(1), partition));
    ASSERT_NE(nullptr, partition);
    search_ctx.set_async_query_ctx(&catalog);
    search_ctx.set_current_async_partition(partition);
    search_ctx.set_async_binding(index_table_id, 0, true);
  }

  static constexpr uint32_t OB_MOCK_EXPR_FRAME_IDX = 0;

protected:
  ObArenaAllocator allocator_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;

  ObDASIRScanCtDef ir_scan_ctdef_;
  ObDASIRScanRtDef ir_scan_rtdef_;
  ObDASScalarScanCtDef inv_idx_scan_ctdef_;
  ObDASScalarScanRtDef inv_idx_scan_rtdef_;

  ObDASMatchCtDef match_ctdef_;
  ObDASMatchRtDef match_rtdef_;
  ObDASMatchPhraseCtDef match_phrase_ctdef_;
  ObDASMatchPhraseRtDef match_phrase_rtdef_;
  ObDASMultiMatchCtDef multi_match_ctdef_;
  ObDASMultiMatchRtDef multi_match_rtdef_;
  ObDASQueryStringCtDef query_string_ctdef_;
  ObDASQueryStringRtDef query_string_rtdef_;
};

} // namespace

// ============================================================================
// Section 1: ObDASInt64LessComp Comparator & Binary Heap Tests
// ============================================================================

TEST_F(ObDASMatchQueryTest, test_int64_less_comp_max_heap)
{
  ObDASInt64LessComp cmp;
  EXPECT_EQ(OB_SUCCESS, cmp.get_error_code());

  EXPECT_TRUE(cmp(10, 20));
  EXPECT_FALSE(cmp(20, 10));
  EXPECT_FALSE(cmp(10, 10));

  ObBinaryHeap<int64_t, ObDASInt64LessComp> max_heap(cmp);
  EXPECT_TRUE(max_heap.empty());
  EXPECT_EQ(0, max_heap.count());

  ASSERT_EQ(OB_SUCCESS, max_heap.push(10));
  ASSERT_EQ(OB_SUCCESS, max_heap.push(50));
  ASSERT_EQ(OB_SUCCESS, max_heap.push(30));
  ASSERT_EQ(OB_SUCCESS, max_heap.push(20));

  EXPECT_EQ(4, max_heap.count());
  EXPECT_EQ(50, max_heap.top()); // Largest element at top

  ASSERT_EQ(OB_SUCCESS, max_heap.pop());
  EXPECT_EQ(30, max_heap.top());

  ASSERT_EQ(OB_SUCCESS, max_heap.pop());
  EXPECT_EQ(20, max_heap.top());

  ASSERT_EQ(OB_SUCCESS, max_heap.pop());
  EXPECT_EQ(10, max_heap.top());

  ASSERT_EQ(OB_SUCCESS, max_heap.pop());
  EXPECT_TRUE(max_heap.empty());
}

TEST_F(ObDASMatchQueryTest, test_query_tokens_tenant_memory_attr)
{
  ObDASMatchRtDef match_rtdef;
  ObDASMultiMatchRtDef multi_match_rtdef;
  ObDASQueryStringRtDef query_string_rtdef;
  EXPECT_EQ(MTL_ID(), match_rtdef.query_tokens_.block_allocator_.get_attr().tenant_id_);
  EXPECT_EQ(MTL_ID(), multi_match_rtdef.query_tokens_.block_allocator_.get_attr().tenant_id_);
  EXPECT_EQ(MTL_ID(), query_string_rtdef.token_groups_.block_allocator_.get_attr().tenant_id_);
  EXPECT_EQ(MTL_ID(), query_string_rtdef.token_boosts_.block_allocator_.get_attr().tenant_id_);
  EXPECT_FALSE(multi_match_rtdef.query_tokens_prepared_);
  EXPECT_FALSE(query_string_rtdef.query_groups_prepared_);
  EXPECT_EQ(0, query_string_rtdef.token_count_);
}

TEST_F(ObDASMatchQueryTest, test_fts_cost_aggregation)
{
  ObSEArray<int64_t, 4> costs;
  int64_t result = -1;
  ASSERT_EQ(OB_SUCCESS, costs.push_back(100));
  ASSERT_EQ(OB_SUCCESS, costs.push_back(50));
  ASSERT_EQ(OB_SUCCESS, costs.push_back(30));

  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_and_cost(costs, result));
  EXPECT_EQ(30, result);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_or_cost(costs, 120, result));
  EXPECT_EQ(120, result);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_msm_cost(costs, 2, 1000, result));
  EXPECT_EQ(80, result);
}

TEST_F(ObDASMatchQueryTest, test_fts_match_cost_aggregation)
{
  ObSEArray<int64_t, 4> costs;
  int64_t result = -1;
  ASSERT_EQ(OB_SUCCESS, costs.push_back(100));
  ASSERT_EQ(OB_SUCCESS, costs.push_back(50));
  ASSERT_EQ(OB_SUCCESS, costs.push_back(30));

  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_match_cost(
      costs, ObMatchOperator::MATCH_OPERATOR_AND, 1, 1000, result));
  EXPECT_EQ(30, result);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_match_cost(
      costs, ObMatchOperator::MATCH_OPERATOR_OR, 1, 120, result));
  EXPECT_EQ(120, result);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_match_cost(
      costs, ObMatchOperator::MATCH_OPERATOR_OR, 2, 1000, result));
  EXPECT_EQ(80, result);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_match_cost(
      costs, ObMatchOperator::MATCH_OPERATOR_OR, 3, 1000, result));
  EXPECT_EQ(30, result);
}

TEST_F(ObDASMatchQueryTest, test_fts_cost_aggregation_with_extended_msm)
{
  ObSEArray<int64_t, 4> costs;
  ObDatum msm_datum;
  int64_t msm_storage = 0;
  int64_t effective_msm = 0;
  int64_t result = -1;
  ASSERT_EQ(OB_SUCCESS, costs.push_back(100));
  ASSERT_EQ(OB_SUCCESS, costs.push_back(50));
  ASSERT_EQ(OB_SUCCESS, costs.push_back(30));

  msm_datum.ptr_ = reinterpret_cast<const char *>(&msm_storage);
  msm_datum.set_int(-1);
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(
      allocator_, costs.count(), false, &msm_datum, effective_msm));
  EXPECT_EQ(2, effective_msm);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_match_cost(
      costs, ObMatchOperator::MATCH_OPERATOR_OR, effective_msm, 1000, result));
  EXPECT_EQ(80, result);

  msm_datum.set_string(ObString::make_string("75%"));
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(
      allocator_, costs.count(), true, &msm_datum, effective_msm));
  EXPECT_EQ(2, effective_msm);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_match_cost(
      costs, ObMatchOperator::MATCH_OPERATOR_OR, effective_msm, 1000, result));
  EXPECT_EQ(80, result);

  msm_datum.set_string(ObString::make_string("2<-25% 9<-1"));
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(
      allocator_, costs.count(), true, &msm_datum, effective_msm));
  EXPECT_EQ(3, effective_msm);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_match_cost(
      costs, ObMatchOperator::MATCH_OPERATOR_OR, effective_msm, 1000, result));
  EXPECT_EQ(30, result);
}

TEST_F(ObDASMatchQueryTest, test_fts_cost_aggregation_invalid_argument)
{
  ObSEArray<int64_t, 4> costs;
  int64_t result = -1;
  ASSERT_EQ(OB_SUCCESS, costs.push_back(100));
  ASSERT_EQ(OB_SUCCESS, costs.push_back(-1));

  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDASFtsCostUtils::aggregate_and_cost(costs, result));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDASFtsCostUtils::aggregate_or_cost(costs, 1000, result));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDASFtsCostUtils::aggregate_msm_cost(costs, 2, 1000, result));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDASFtsCostUtils::aggregate_msm_cost(costs, 3, 1000, result));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDASFtsCostUtils::aggregate_match_cost(
      costs, ObMatchOperator::MAX_MATCH_OPERATOR, 1, 1000, result));
}

TEST_F(ObDASMatchQueryTest, test_fts_cost_aggregation_boundary)
{
  ObSEArray<int64_t, 4> costs;
  int64_t result = -1;
  ASSERT_EQ(OB_SUCCESS, costs.push_back(INT64_MAX));
  ASSERT_EQ(OB_SUCCESS, costs.push_back(INT64_MAX));

  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_or_cost(costs, 1000, result));
  EXPECT_EQ(1000, result);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_msm_cost(costs, 2, 1000, result));
  EXPECT_EQ(1000, result);

  costs.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_and_cost(costs, result));
  EXPECT_EQ(0, result);
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::aggregate_or_cost(costs, 0, result));
  EXPECT_EQ(0, result);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDASFtsCostUtils::aggregate_or_cost(costs, -1, result));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDASFtsCostUtils::aggregate_msm_cost(costs, 1, 1000, result));
}

TEST_F(ObDASMatchQueryTest, test_async_estimator_switch_query_token)
{
  SearchCtxSetup ctx_setup(allocator_);
  storage::ObAsyncBM25ParamEstimator estimator;
  ASSERT_EQ(OB_SUCCESS, estimator.init(
      ctx_setup.search_ctx_, ir_scan_ctdef_, ir_scan_rtdef_,
      ObTabletID(1001), ObString::make_string("ocean")));
  estimator.token_doc_cnt_ = 100;
  estimator.estimated_ = true;
  estimator.inv_scan_param_inited_ = true;

  ASSERT_EQ(OB_SUCCESS, estimator.switch_query_token(ObString::make_string("base")));
  EXPECT_EQ(ObString::make_string("base"), estimator.query_token_);
  EXPECT_EQ(0, estimator.token_doc_cnt_);
  EXPECT_FALSE(estimator.estimated_);
  EXPECT_TRUE(estimator.inv_scan_param_inited_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, estimator.switch_query_token(ObString()));
}

// ============================================================================
// Section 2: prepare_query_tokens Tests (Tokenize & Caching)
// ============================================================================

TEST_F(ObDASMatchQueryTest, test_prepare_query_tokens_caching)
{
  EXPECT_FALSE(match_rtdef_.query_tokens_prepared_);
  EXPECT_TRUE(match_rtdef_.query_tokens_.empty());

  ASSERT_EQ(OB_SUCCESS, match_rtdef_.prepare_query_tokens());
  EXPECT_TRUE(match_rtdef_.query_tokens_prepared_);
  EXPECT_FALSE(match_rtdef_.query_tokens_.empty());
  int64_t token_cnt = match_rtdef_.query_tokens_.count();
  EXPECT_GT(token_cnt, 0);

  // Second call should reuse cached result directly
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.prepare_query_tokens());
  EXPECT_EQ(token_cnt, match_rtdef_.query_tokens_.count());
}

TEST_F(ObDASMatchQueryTest, test_prepare_query_tokens_dedup_with_counts)
{
  set_query_text("ocean ocean base");
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.prepare_query_tokens());
  EXPECT_TRUE(match_rtdef_.query_tokens_prepared_);
  // Duplicate query tokens are deduplicated, with occurrence counts recorded in
  // the parallel token_counts_ array and summed into total_token_cnt_.
  ASSERT_EQ(2, match_rtdef_.query_tokens_.count());
  ASSERT_EQ(2, match_rtdef_.token_counts_.count());
  EXPECT_EQ(3, match_rtdef_.total_token_cnt_);
  for (int64_t i = 0; i < match_rtdef_.query_tokens_.count(); ++i) {
    const ObString &token = match_rtdef_.query_tokens_.at(i);
    if (token == ObString::make_string("ocean")) {
      EXPECT_EQ(2, match_rtdef_.token_counts_.at(i));
    } else if (token == ObString::make_string("base")) {
      EXPECT_EQ(1, match_rtdef_.token_counts_.at(i));
    } else {
      ADD_FAILURE() << "unexpected query token";
    }
  }
}

TEST_F(ObDASMatchQueryTest, test_compute_cost_async_or_with_duplicate_tokens)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean ocean base");
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(1);

  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);

  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("base"), 50));

  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_TRUE(cost.is_valid());
  // OR operator (msm <= 1): deduplicated sum(100, 50) = 150; the duplicate
  // "ocean" is neither estimated nor counted twice.
  EXPECT_EQ(150, cost.cost());
  ASSERT_EQ(2, match_rtdef_.query_tokens_.count());
  ASSERT_EQ(2, match_rtdef_.token_costs_.count());
  EXPECT_EQ(3, match_rtdef_.total_token_cnt_);
}

TEST_F(ObDASMatchQueryTest, test_compute_cost_async_msm_with_duplicate_tokens)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean ocean base"); // unique tokens: ocean x2, base x1
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(2); // msm = 2, total clauses = 3, least_clause_cnt = 2

  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);

  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  // msm > 1 keeps per-occurrence clause semantics: expanded clause costs are
  // ocean=100, ocean=100, base=50; smallest 2 costs: 50, 100 -> sum = 150.
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("base"), 50));

  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_TRUE(cost.is_valid());
  EXPECT_EQ(150, cost.cost());
  // token_costs_ stays aligned with the deduplicated tokens.
  ASSERT_EQ(match_rtdef_.query_tokens_.count(), match_rtdef_.token_costs_.count());
  ASSERT_EQ(2, match_rtdef_.token_costs_.count());
}

TEST_F(ObDASMatchQueryTest, test_copy_cached_runtime_state_with_token_counts)
{
  set_query_text("ocean ocean base");
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.prepare_query_tokens());
  ASSERT_EQ(2, match_rtdef_.query_tokens_.count());

  ObDASMatchRtDef cloned_rtdef;
  ASSERT_EQ(OB_SUCCESS, cloned_rtdef.copy_cached_runtime_state_from(match_rtdef_));
  EXPECT_TRUE(cloned_rtdef.query_tokens_prepared_);
  EXPECT_EQ(3, cloned_rtdef.total_token_cnt_);
  ASSERT_EQ(match_rtdef_.token_counts_.count(), cloned_rtdef.token_counts_.count());
  for (int64_t i = 0; i < cloned_rtdef.token_counts_.count(); ++i) {
    EXPECT_EQ(match_rtdef_.token_counts_.at(i), cloned_rtdef.token_counts_.at(i));
  }
  ASSERT_EQ(match_rtdef_.query_tokens_.count(), cloned_rtdef.query_tokens_.count());
  for (int64_t i = 0; i < cloned_rtdef.query_tokens_.count(); ++i) {
    EXPECT_EQ(match_rtdef_.query_tokens_.at(i), cloned_rtdef.query_tokens_.at(i));
    EXPECT_NE(match_rtdef_.query_tokens_.at(i).ptr(), cloned_rtdef.query_tokens_.at(i).ptr());
  }
}

TEST_F(ObDASMatchQueryTest, test_prepare_query_tokens_null_query_text)
{
  set_query_text(nullptr);
  EXPECT_EQ(OB_ERR_UNEXPECTED, match_rtdef_.prepare_query_tokens());
}

TEST_F(ObDASMatchQueryTest, test_prepare_query_tokens_logical_table_not_supported)
{
  inv_idx_scan_ctdef_.logical_table_aux_type_ = share::LT_AUX_FTS_INVERTED;
  EXPECT_EQ(OB_NOT_SUPPORTED, match_rtdef_.prepare_query_tokens());
}

TEST_F(ObDASMatchQueryTest, test_get_cost_reuses_runtime_cache)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("");
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;

  ASSERT_EQ(OB_SUCCESS, match_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(0, cost.cost());
  EXPECT_TRUE(match_rtdef_.query_tokens_prepared_);
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(0, cost.cost());
  EXPECT_TRUE(multi_match_rtdef_.query_tokens_prepared_);
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(0, cost.cost());
  EXPECT_TRUE(query_string_rtdef_.query_groups_prepared_);

  set_query_text("lake base");
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(0, cost.cost());
  EXPECT_TRUE(match_rtdef_.query_tokens_.empty());
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(0, cost.cost());
  EXPECT_TRUE(multi_match_rtdef_.query_tokens_.empty());
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(0, cost.cost());
  EXPECT_TRUE(query_string_rtdef_.token_groups_.empty());
}

// ============================================================================
// Section 3: compute_cost Tests
// ============================================================================

TEST_F(ObDASMatchQueryTest, test_compute_cost_non_async_index_invalid_cost)
{
  ir_scan_ctdef_.is_async_index_ = false;

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_FALSE(cost.is_valid()); // Non-async match yields invalid cost -> fallback to table row count
}

TEST_F(ObDASMatchQueryTest, test_compute_cost_async_empty_tokens)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("");

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_TRUE(cost.is_valid());
  EXPECT_EQ(0, cost.cost());
}

TEST_F(ObDASMatchQueryTest, test_compute_cost_preserves_prepare_failure)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text(nullptr);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost(123);
  EXPECT_EQ(OB_ERR_UNEXPECTED, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_FALSE(cost.is_valid());
}

TEST_F(ObDASMatchQueryTest, test_compute_cost_rejects_null_ctdef)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost(123);
  match_rtdef_.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_FALSE(cost.is_valid());

  cost = ObDASSearchCost(123);
  multi_match_rtdef_.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, multi_match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_FALSE(cost.is_valid());

  cost = ObDASSearchCost(123);
  query_string_rtdef_.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, query_string_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_FALSE(cost.is_valid());
}

TEST_F(ObDASMatchQueryTest, test_prepare_rejects_null_query_expr)
{
  ObExpr *match_query_text = match_ctdef_.query_text_;
  ObExpr *multi_match_query_text = multi_match_ctdef_.query_text_;
  ObExpr *query_string_query_text = query_string_ctdef_.query_text_;

  match_ctdef_.query_text_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, match_rtdef_.prepare_query_tokens());
  match_ctdef_.query_text_ = match_query_text;

  multi_match_ctdef_.query_text_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, multi_match_rtdef_.prepare_query_tokens());
  multi_match_ctdef_.query_text_ = multi_match_query_text;

  query_string_ctdef_.query_text_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, query_string_rtdef_.prepare_query_groups());
  query_string_ctdef_.query_text_ = query_string_query_text;
}

TEST_F(ObDASMatchQueryTest, test_compute_cost_rejects_null_operator_expr)
{
  ir_scan_ctdef_.is_async_index_ = true;
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObDASSearchCost cost;

  match_rtdef_.query_tokens_prepared_ = true;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.query_tokens_.push_back(ObString::make_string("token")));
  ObExpr *match_operator = match_ctdef_.match_operator_;
  match_ctdef_.match_operator_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  match_ctdef_.match_operator_ = match_operator;

  multi_match_rtdef_.query_tokens_prepared_ = true;
  ASSERT_EQ(OB_SUCCESS,
      multi_match_rtdef_.query_tokens_.push_back(ObString::make_string("token")));
  ObExpr *multi_match_operator = multi_match_ctdef_.match_operator_;
  multi_match_ctdef_.match_operator_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, multi_match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  multi_match_ctdef_.match_operator_ = multi_match_operator;

  set_query_text("token");
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.prepare_query_groups());
  ObExpr *default_operator = query_string_ctdef_.default_operator_;
  query_string_ctdef_.default_operator_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, query_string_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  query_string_ctdef_.default_operator_ = default_operator;
}

TEST_F(ObDASMatchQueryTest, test_compute_cost_async_with_cached_doc_cnt)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean base");
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_AND);

  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);

  // Inject token doc counts into bm25_param_cache_
  ObTabletID tablet_id(1001);
  inv_idx_scan_ctdef_.ref_table_id_ = 100;
  // Set table_param_ schema
  inv_idx_scan_ctdef_.table_param_.set_is_partition_table(false);

  // Mock get_related_tablet_id in search_ctx
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  
  // Fill cache: token "ocean" -> 100, token "base" -> 50
  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(tablet_id, ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(tablet_id, ObString::make_string("base"), 50));

  // In test environment, get_related_tablet_id will return tablet_id = 0, so prefill for default tablet_id(0) as well
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("base"), 50));

  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_TRUE(cost.is_valid());
  // AND operator: min(100, 50) = 50
  EXPECT_EQ(50, cost.cost());
}

TEST_F(ObDASMatchQueryTest, test_match_phrase_compute_cost_non_async_index)
{
  ir_scan_ctdef_.is_async_index_ = false;
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(321);
  ObDASSearchCost cost;

  ASSERT_EQ(OB_SUCCESS, match_phrase_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_FALSE(cost.is_valid());
  ASSERT_EQ(OB_SUCCESS, match_phrase_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(321, cost.cost());
}

TEST_F(ObDASMatchQueryTest, test_match_phrase_compute_cost_empty_tokens)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("");
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;

  ASSERT_EQ(OB_SUCCESS, match_phrase_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(0, cost.cost());
  EXPECT_TRUE(match_phrase_rtdef_.query_tokens_prepared_);
  EXPECT_TRUE(match_phrase_rtdef_.query_tokens_.empty());
}

TEST_F(ObDASMatchQueryTest, test_match_phrase_compute_cost_async_with_cached_doc_cnt)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean base");
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("base"), 50));
  ObDASSearchCost cost;

  ASSERT_EQ(OB_SUCCESS, match_phrase_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(50, cost.cost());
  EXPECT_TRUE(match_phrase_rtdef_.query_tokens_prepared_);
  EXPECT_EQ(2, match_phrase_rtdef_.query_tokens_.count());
  EXPECT_EQ(match_phrase_rtdef_.query_tokens_.count(), match_phrase_rtdef_.token_ids_.count());
  EXPECT_EQ(match_phrase_rtdef_.query_tokens_.count(), match_phrase_rtdef_.token_offsets_.count());
}

TEST_F(ObDASMatchQueryTest, test_match_phrase_compute_cost_caps_at_table_rows)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean");
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(40);
  ASSERT_EQ(OB_SUCCESS, ctx_setup.search_ctx_.get_bm25_param_cache().set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("ocean"), 100));
  ObDASSearchCost cost;

  ASSERT_EQ(OB_SUCCESS, match_phrase_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(40, cost.cost());
}

TEST_F(ObDASMatchQueryTest, test_match_phrase_runtime_cache_and_clone)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean ocean base");
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("base"), 50));
  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, match_phrase_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  ASSERT_EQ(50, cost.cost());
  ASSERT_TRUE(match_phrase_rtdef_.has_duplicate_tokens_);

  // The prepared tokens are reused even if the datum value changes.
  set_query_text("changed query");
  ASSERT_EQ(OB_SUCCESS, match_phrase_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(50, cost.cost());
  EXPECT_EQ(2, match_phrase_rtdef_.query_tokens_.count());

  ObDASMatchPhraseRtDef cloned_rtdef;
  ASSERT_EQ(OB_SUCCESS, cloned_rtdef.copy_cached_runtime_state_from(match_phrase_rtdef_));
  EXPECT_EQ(50, cloned_rtdef.cost_.cost());
  EXPECT_TRUE(cloned_rtdef.query_tokens_prepared_);
  EXPECT_TRUE(cloned_rtdef.has_duplicate_tokens_);
  ASSERT_EQ(match_phrase_rtdef_.query_tokens_.count(), cloned_rtdef.query_tokens_.count());
  ASSERT_EQ(match_phrase_rtdef_.token_ids_.count(), cloned_rtdef.token_ids_.count());
  ASSERT_EQ(match_phrase_rtdef_.token_offsets_.count(), cloned_rtdef.token_offsets_.count());
  for (int64_t i = 0; i < cloned_rtdef.query_tokens_.count(); ++i) {
    EXPECT_EQ(match_phrase_rtdef_.query_tokens_.at(i), cloned_rtdef.query_tokens_.at(i));
    EXPECT_NE(match_phrase_rtdef_.query_tokens_.at(i).ptr(), cloned_rtdef.query_tokens_.at(i).ptr());
  }
}

TEST_F(ObDASMatchQueryTest, test_match_phrase_compute_cost_preserves_prepare_failure)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text(nullptr);
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost(123);

  EXPECT_EQ(OB_ERR_UNEXPECTED, match_phrase_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_FALSE(cost.is_valid());
  EXPECT_FALSE(match_phrase_rtdef_.query_tokens_prepared_);
}

TEST_F(ObDASMatchQueryTest, test_compute_cost_async_or_operator)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean base");
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(1);

  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);

  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("base"), 50));

  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_TRUE(cost.is_valid());
  // OR operator (msm <= 1): sum(100, 50) = 150
  EXPECT_EQ(150, cost.cost());
}

TEST_F(ObDASMatchQueryTest, test_compute_cost_async_msm_operator)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean base database"); // 3 tokens
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(2); // msm = 2, tokens = 3, least_clause_cnt = 3 - 2 + 1 = 2

  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);

  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  // token costs: ocean=100, base=50, database=30
  // smallest 2 costs: 30, 50 -> sum = 80
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("base"), 50));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("database"), 30));

  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_TRUE(cost.is_valid());
  EXPECT_EQ(80, cost.cost());
  ASSERT_EQ(match_rtdef_.query_tokens_.count(), match_rtdef_.token_costs_.count());
  for (int64_t i = 0; i < match_rtdef_.query_tokens_.count(); ++i) {
    const ObString &token = match_rtdef_.query_tokens_.at(i);
    const int64_t expected_cost = token == ObString::make_string("ocean") ? 100
        : token == ObString::make_string("base") ? 50
        : token == ObString::make_string("database") ? 30
        : -1;
    EXPECT_NE(-1, expected_cost);
    EXPECT_EQ(expected_cost, match_rtdef_.token_costs_.at(i).cost());
  }
}

TEST_F(ObDASMatchQueryTest, test_multi_match_cost_and_cache)
{
  ir_scan_ctdef_.is_async_index_ = true;
  multi_match_ctdef_.set_is_top_level_scoring(true);
  set_query_text("ocean base");
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(1);
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("base"), 50));

  ObDASSearchCost cost(123);
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  ASSERT_TRUE(cost.is_valid());
  EXPECT_EQ(150, cost.cost());
  EXPECT_TRUE(multi_match_rtdef_.query_tokens_prepared_);
  const int64_t token_count = multi_match_rtdef_.query_tokens_.count();
  ASSERT_EQ(token_count, multi_match_rtdef_.field_token_costs_.count());
  EXPECT_EQ(100, multi_match_rtdef_.field_token_costs_.at(0).cost());
  EXPECT_EQ(50, multi_match_rtdef_.field_token_costs_.at(1).cost());
  ASSERT_EQ(1, multi_match_rtdef_.field_costs_.count());
  EXPECT_EQ(150, multi_match_rtdef_.field_costs_.at(0).cost());
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef_.prepare_query_tokens());
  EXPECT_EQ(token_count, multi_match_rtdef_.query_tokens_.count());

  ObDASMultiMatchRtDef cloned_rtdef;
  ASSERT_EQ(OB_SUCCESS, cloned_rtdef.copy_cached_runtime_state_from(multi_match_rtdef_));
  EXPECT_EQ(150, cloned_rtdef.cost_.cost());
  EXPECT_TRUE(cloned_rtdef.query_tokens_prepared_);
  ASSERT_EQ(token_count, cloned_rtdef.query_tokens_.count());
  EXPECT_NE(multi_match_rtdef_.query_tokens_.at(0).ptr(), cloned_rtdef.query_tokens_.at(0).ptr());
  ASSERT_EQ(token_count, cloned_rtdef.field_token_costs_.count());
  EXPECT_EQ(100, cloned_rtdef.field_token_costs_.at(0).cost());
  EXPECT_EQ(50, cloned_rtdef.field_token_costs_.at(1).cost());
  ASSERT_EQ(1, cloned_rtdef.field_costs_.count());
  EXPECT_EQ(150, cloned_rtdef.field_costs_.at(0).cost());
}

TEST_F(ObDASMatchQueryTest, test_multi_match_two_field_cost)
{
  ObDASIRScanCtDef second_ir_ctdef(allocator_);
  ObDASIRScanRtDef second_ir_rtdef;
  ObDASScalarScanCtDef second_inv_ctdef(allocator_);
  ObDASScalarScanRtDef second_inv_rtdef;
  second_ir_ctdef.is_async_index_ = true;
  second_ir_ctdef.use_scalar_scan_def_ = true;
  second_ir_rtdef.ctdef_ = &second_ir_ctdef;
  second_inv_ctdef.table_param_.parser_name_ = ObString::make_string("space.1");
  second_inv_ctdef.table_param_.parser_properties_ =
      inv_idx_scan_ctdef_.table_param_.parser_properties_;
  second_inv_ctdef.table_param_.fts_index_type_ = share::schema::OB_FTS_INDEX_TYPE_MATCH;
  void *second_ir_children = allocator_.alloc(sizeof(ObDASBaseCtDef *));
  second_ir_ctdef.children_ = static_cast<ObDASBaseCtDef **>(second_ir_children);
  second_ir_ctdef.children_cnt_ = 1;
  second_ir_ctdef.children_[0] = &second_inv_ctdef;

  void *ct_children = allocator_.alloc(sizeof(ObDASBaseCtDef *) * 2);
  void *rt_children = allocator_.alloc(sizeof(ObDASBaseRtDef *) * 2);
  multi_match_ctdef_.children_ = static_cast<ObDASBaseCtDef **>(ct_children);
  multi_match_ctdef_.children_cnt_ = 2;
  multi_match_ctdef_.children_[0] = &ir_scan_ctdef_;
  multi_match_ctdef_.children_[1] = &second_ir_ctdef;
  multi_match_rtdef_.children_ = static_cast<ObDASBaseRtDef **>(rt_children);
  multi_match_rtdef_.children_cnt_ = 2;
  multi_match_rtdef_.children_[0] = &ir_scan_rtdef_;
  multi_match_rtdef_.children_[1] = &second_ir_rtdef;
  multi_match_ctdef_.ir_ctdef_indices_.reset();
  ASSERT_EQ(OB_SUCCESS, multi_match_ctdef_.ir_ctdef_indices_.init(2));
  ASSERT_EQ(OB_SUCCESS, multi_match_ctdef_.ir_ctdef_indices_.prepare_allocate(2));
  multi_match_ctdef_.ir_ctdef_indices_.at(0) = 0;
  multi_match_ctdef_.ir_ctdef_indices_.at(1) = 1;

  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean base database");
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(2);
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.mock_scan_op_.get_related_ctdefs().set_capacity(2);
  ctx_setup.mock_scan_op_.get_related_rtdefs().set_capacity(2);
  ctx_setup.mock_scan_op_.get_related_tablet_ids().set_capacity(2);
  ASSERT_EQ(OB_SUCCESS, ctx_setup.mock_scan_op_.get_related_ctdefs().push_back(&inv_idx_scan_ctdef_));
  ASSERT_EQ(OB_SUCCESS, ctx_setup.mock_scan_op_.get_related_rtdefs().push_back(&inv_idx_scan_rtdef_));
  ASSERT_EQ(OB_SUCCESS, ctx_setup.mock_scan_op_.get_related_tablet_ids().push_back(ObTabletID(1001)));
  ASSERT_EQ(OB_SUCCESS, ctx_setup.mock_scan_op_.get_related_ctdefs().push_back(&second_inv_ctdef));
  ASSERT_EQ(OB_SUCCESS, ctx_setup.mock_scan_op_.get_related_rtdefs().push_back(&second_inv_rtdef));
  ASSERT_EQ(OB_SUCCESS, ctx_setup.mock_scan_op_.get_related_tablet_ids().push_back(ObTabletID(1002)));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1001), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1001), ObString::make_string("base"), 50));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1001), ObString::make_string("database"), 30));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1002), ObString::make_string("ocean"), 40));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1002), ObString::make_string("base"), 20));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1002), ObString::make_string("database"), 10));

  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(110, cost.cost());

  set_match_operator(ObMatchOperator::MATCH_OPERATOR_AND);
  cost.reset();
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(40, cost.cost());

  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(2);
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(100);
  cost.reset();
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(100, cost.cost());
}

TEST_F(ObDASMatchQueryTest, test_multi_match_cost_and_empty_and_error)
{
  ir_scan_ctdef_.is_async_index_ = true;
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost(123);
  set_query_text("");
  ASSERT_EQ(OB_SUCCESS, multi_match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  ASSERT_TRUE(cost.is_valid());
  EXPECT_EQ(0, cost.cost());

  set_query_text(nullptr);
  multi_match_rtdef_.query_tokens_prepared_ = false;
  multi_match_rtdef_.query_tokens_.reuse();
  cost = ObDASSearchCost(123);
  EXPECT_EQ(OB_ERR_UNEXPECTED, multi_match_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_FALSE(cost.is_valid());
}

TEST_F(ObDASMatchQueryTest, test_query_string_cost_group_operator_and_cache)
{
  ir_scan_ctdef_.is_async_index_ = true;
  query_string_ctdef_.set_is_top_level_scoring(true);
  set_query_text("ocean base");
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(1);
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("base"), 50));

  ObDASSearchCost cost(123);
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.get_cost(ctx_setup.search_ctx_, cost));
  ASSERT_TRUE(cost.is_valid());
  EXPECT_EQ(150, cost.cost());
  EXPECT_TRUE(query_string_rtdef_.query_groups_prepared_);
  EXPECT_EQ(2, query_string_rtdef_.token_count_);
  const int64_t group_count = query_string_rtdef_.token_groups_.count();
  ASSERT_EQ(1, group_count);
  ASSERT_EQ(2, query_string_rtdef_.field_token_costs_.count());
  EXPECT_EQ(100, query_string_rtdef_.field_token_costs_.at(0).cost());
  EXPECT_EQ(50, query_string_rtdef_.field_token_costs_.at(1).cost());
  ASSERT_EQ(1, query_string_rtdef_.field_costs_.count());
  EXPECT_EQ(150, query_string_rtdef_.field_costs_.at(0).cost());
  ASSERT_EQ(group_count, query_string_rtdef_.group_costs_.count());
  EXPECT_EQ(150, query_string_rtdef_.group_costs_.at(0).cost());
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.prepare_query_groups());
  EXPECT_EQ(group_count, query_string_rtdef_.token_groups_.count());

  ObDASQueryStringRtDef cloned_rtdef;
  ASSERT_EQ(OB_SUCCESS, cloned_rtdef.copy_cached_runtime_state_from(query_string_rtdef_));
  EXPECT_EQ(150, cloned_rtdef.cost_.cost());
  EXPECT_TRUE(cloned_rtdef.query_groups_prepared_);
  ASSERT_EQ(group_count, cloned_rtdef.token_groups_.count());
  EXPECT_NE(query_string_rtdef_.token_groups_.at(0).at(0).ptr(),
            cloned_rtdef.token_groups_.at(0).at(0).ptr());
  ASSERT_EQ(group_count, cloned_rtdef.group_costs_.count());
  EXPECT_EQ(150, cloned_rtdef.group_costs_.at(0).cost());

  set_match_operator(ObMatchOperator::MATCH_OPERATOR_AND);
  set_minimum_should_match(1);
  query_string_rtdef_.query_groups_prepared_ = false;
  query_string_rtdef_.token_groups_.reuse();
  query_string_rtdef_.token_boosts_.reuse();
  query_string_rtdef_.token_count_ = 0;
  cost.reset();
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(50, cost.cost());
}

TEST_F(ObDASMatchQueryTest, test_query_string_multi_group_msm_cost)
{
  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean^2 base^3 database^4");
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(2);
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("base"), 50));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(0), ObString::make_string("database"), 30));

  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(80, cost.cost());
  EXPECT_EQ(3, query_string_rtdef_.token_groups_.count());
  EXPECT_EQ(3, query_string_rtdef_.token_boosts_.count());
  EXPECT_EQ(3, query_string_rtdef_.token_count_);
}

TEST_F(ObDASMatchQueryTest, test_query_string_multi_group_multi_field_cost)
{
  ObDASIRScanCtDef second_ir_ctdef(allocator_);
  ObDASIRScanRtDef second_ir_rtdef;
  ObDASScalarScanCtDef second_inv_ctdef(allocator_);
  ObDASScalarScanRtDef second_inv_rtdef;
  second_ir_ctdef.is_async_index_ = true;
  second_ir_ctdef.use_scalar_scan_def_ = true;
  second_ir_rtdef.ctdef_ = &second_ir_ctdef;
  second_inv_ctdef.table_param_.parser_name_ = ObString::make_string("space.1");
  second_inv_ctdef.table_param_.parser_properties_ =
      inv_idx_scan_ctdef_.table_param_.parser_properties_;
  second_inv_ctdef.table_param_.fts_index_type_ = share::schema::OB_FTS_INDEX_TYPE_MATCH;
  ObDASBaseCtDef *second_ir_children[] = {&second_inv_ctdef};
  second_ir_ctdef.children_ = second_ir_children;
  second_ir_ctdef.children_cnt_ = ARRAYSIZEOF(second_ir_children);

  ObDASBaseCtDef *ctdef_children[] = {&ir_scan_ctdef_, &second_ir_ctdef};
  ObDASBaseRtDef *rtdef_children[] = {&ir_scan_rtdef_, &second_ir_rtdef};
  query_string_ctdef_.children_ = ctdef_children;
  query_string_ctdef_.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  query_string_rtdef_.children_ = rtdef_children;
  query_string_rtdef_.children_cnt_ = ARRAYSIZEOF(rtdef_children);
  query_string_ctdef_.ir_ctdef_indices_.reset();
  ASSERT_EQ(OB_SUCCESS, query_string_ctdef_.ir_ctdef_indices_.init(2));
  ASSERT_EQ(OB_SUCCESS, query_string_ctdef_.ir_ctdef_indices_.prepare_allocate(2));
  query_string_ctdef_.ir_ctdef_indices_.at(0) = 0;
  query_string_ctdef_.ir_ctdef_indices_.at(1) = 1;

  ir_scan_ctdef_.is_async_index_ = true;
  set_query_text("ocean^2 base^3");
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(1);
  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.mock_scan_op_.get_related_ctdefs().set_capacity(2);
  ctx_setup.mock_scan_op_.get_related_rtdefs().set_capacity(2);
  ctx_setup.mock_scan_op_.get_related_tablet_ids().set_capacity(2);
  ASSERT_EQ(OB_SUCCESS,
      ctx_setup.mock_scan_op_.get_related_ctdefs().push_back(&inv_idx_scan_ctdef_));
  ASSERT_EQ(OB_SUCCESS,
      ctx_setup.mock_scan_op_.get_related_rtdefs().push_back(&inv_idx_scan_rtdef_));
  ASSERT_EQ(OB_SUCCESS,
      ctx_setup.mock_scan_op_.get_related_tablet_ids().push_back(ObTabletID(1001)));
  ASSERT_EQ(OB_SUCCESS,
      ctx_setup.mock_scan_op_.get_related_ctdefs().push_back(&second_inv_ctdef));
  ASSERT_EQ(OB_SUCCESS,
      ctx_setup.mock_scan_op_.get_related_rtdefs().push_back(&second_inv_rtdef));
  ASSERT_EQ(OB_SUCCESS,
      ctx_setup.mock_scan_op_.get_related_tablet_ids().push_back(ObTabletID(1002)));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1001), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1001), ObString::make_string("base"), 50));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1002), ObString::make_string("ocean"), 40));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(
      ObTabletID(1002), ObString::make_string("base"), 20));

  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(210, cost.cost());
  EXPECT_EQ(2, query_string_rtdef_.token_groups_.count());

  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(100);
  cost.reset();
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(100, cost.cost());
}

TEST_F(ObDASMatchQueryTest, test_query_string_cost_empty_and_error)
{
  ir_scan_ctdef_.is_async_index_ = true;
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost(123);
  set_query_text("");
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  ASSERT_TRUE(cost.is_valid());
  EXPECT_EQ(0, cost.cost());

  query_string_rtdef_.token_count_ = 1;
  cost = ObDASSearchCost(123);
  ASSERT_EQ(OB_SUCCESS, query_string_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  ASSERT_TRUE(cost.is_valid());
  EXPECT_EQ(0, cost.cost());

  set_query_text(nullptr);
  query_string_rtdef_.query_groups_prepared_ = false;
  query_string_rtdef_.token_groups_.reuse();
  query_string_rtdef_.token_boosts_.reuse();
  query_string_rtdef_.token_count_ = 0;
  cost = ObDASSearchCost(123);
  EXPECT_EQ(OB_ERR_UNEXPECTED, query_string_rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_FALSE(cost.is_valid());
}

// ============================================================================
// Section 4: generate_op Tests (Operator Tree Generation & Token Cache Reuse)
// ============================================================================

TEST_F(ObDASMatchQueryTest, test_bmm_param_with_lead_cost)
{
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ObSEArray<ObDASSearchCost, 1> score_op_costs;
  const ObDASSearchCost lead_cost(123);
  ObDASBMMBuildOptions param_options;
  param_options.lead_cost_ = lead_cost;
  ObDASBMMOpParam param(score_ops, nullptr, allocator_, param_options, &score_op_costs);

  ASSERT_TRUE(param.is_valid());
  ASSERT_TRUE(param.get_lead_cost().is_valid());
  EXPECT_EQ(123, param.get_lead_cost().cost());
}

TEST_F(ObDASMatchQueryTest, test_bmm_param_with_score_and_filter_costs)
{
  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ObSEArray<ObDASSearchCost, 2> score_op_costs;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(nullptr));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(nullptr));
  ASSERT_EQ(OB_SUCCESS, score_op_costs.push_back(ObDASSearchCost(100)));
  ASSERT_EQ(OB_SUCCESS, score_op_costs.push_back(ObDASSearchCost()));
  ObDASBMMBuildOptions param_options;
  param_options.lead_cost_ = ObDASSearchCost(20);
  param_options.filter_cost_ = ObDASSearchCost(10);
  ObDASBMMOpParam param(score_ops, nullptr, allocator_, param_options, &score_op_costs);

  ASSERT_TRUE(param.is_valid());
  ASSERT_NE(nullptr, param.get_score_op_costs());
  EXPECT_EQ(2, param.get_score_op_costs()->count());
  EXPECT_EQ(100, param.get_score_op_costs()->at(0).cost());
  EXPECT_FALSE(param.get_score_op_costs()->at(1).is_valid());
  EXPECT_EQ(10, param.get_filter_cost().cost());
  EXPECT_EQ(20, param.get_lead_cost().cost());
}

TEST_F(ObDASMatchQueryTest, test_bmm_param_rejects_mismatched_score_costs)
{
  ObSEArray<ObIDASSearchOp *, 2> score_ops;
  ObSEArray<ObDASSearchCost, 1> score_op_costs;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(nullptr));
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(nullptr));
  ASSERT_EQ(OB_SUCCESS, score_op_costs.push_back(ObDASSearchCost(100)));
  ObDASBMMBuildOptions param_options;
  ObDASBMMOpParam param(score_ops, nullptr, allocator_, param_options, &score_op_costs);

  EXPECT_FALSE(param.is_valid());
}

TEST_F(ObDASMatchQueryTest, test_align_token_costs_sync_fills_invalid_costs)
{
  ir_scan_ctdef_.is_async_index_ = false;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.query_tokens_.push_back(ObString::make_string("ocean")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.query_tokens_.push_back(ObString::make_string("base")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.align_token_costs());

  ASSERT_EQ(2, match_rtdef_.token_costs_.count());
  EXPECT_FALSE(match_rtdef_.token_costs_.at(0).is_valid());
  EXPECT_FALSE(match_rtdef_.token_costs_.at(1).is_valid());
}

TEST_F(ObDASMatchQueryTest, test_align_token_costs_async_rejects_mismatched_costs)
{
  ir_scan_ctdef_.is_async_index_ = true;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.query_tokens_.push_back(ObString::make_string("ocean")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.query_tokens_.push_back(ObString::make_string("base")));

  EXPECT_EQ(OB_ERR_UNEXPECTED, match_rtdef_.align_token_costs());
  EXPECT_TRUE(match_rtdef_.token_costs_.empty());
}

TEST_F(ObDASMatchQueryTest, test_align_token_costs_async_rejects_invalid_cost)
{
  ir_scan_ctdef_.is_async_index_ = true;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.query_tokens_.push_back(ObString::make_string("ocean")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.query_tokens_.push_back(ObString::make_string("base")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.token_costs_.push_back(ObDASSearchCost(100)));
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.token_costs_.push_back(ObDASSearchCost()));

  EXPECT_EQ(OB_ERR_UNEXPECTED, match_rtdef_.align_token_costs());
}

TEST_F(ObDASMatchQueryTest, test_align_token_costs_async_accepts_valid_costs)
{
  ir_scan_ctdef_.is_async_index_ = true;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.query_tokens_.push_back(ObString::make_string("ocean")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.query_tokens_.push_back(ObString::make_string("base")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.token_costs_.push_back(ObDASSearchCost(100)));
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.token_costs_.push_back(ObDASSearchCost(50)));

  ASSERT_EQ(OB_SUCCESS, match_rtdef_.align_token_costs());
  ASSERT_EQ(2, match_rtdef_.token_costs_.count());
  EXPECT_EQ(100, match_rtdef_.token_costs_.at(0).cost());
  EXPECT_EQ(50, match_rtdef_.token_costs_.at(1).cost());
}

TEST_F(ObDASMatchQueryTest, test_should_use_bmm_op_by_operator_shape)
{
  EXPECT_TRUE(ObDASMatchRtDef::should_use_bmm_op(
      true, ObMatchOperator::MATCH_OPERATOR_OR, 1)); // BMM
  EXPECT_TRUE(ObDASMatchRtDef::should_use_bmm_op(
      true, ObMatchOperator::MATCH_OPERATOR_OR, 0)); // BMM
  EXPECT_FALSE(ObDASMatchRtDef::should_use_bmm_op(
      true, ObMatchOperator::MATCH_OPERATOR_AND, 1)); // Conjunction
  EXPECT_FALSE(ObDASMatchRtDef::should_use_bmm_op(
      true, ObMatchOperator::MATCH_OPERATOR_OR, 2)); // BMW
  EXPECT_FALSE(ObDASMatchRtDef::should_use_bmm_op(
      false, ObMatchOperator::MATCH_OPERATOR_OR, 1)); // non-top-level Disjunction
}

TEST_F(ObDASMatchQueryTest, test_bmm_param_without_lead_cost)
{
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ObDASBMMOpParam param(score_ops, nullptr, allocator_, ObDASBMMBuildOptions());

  ASSERT_TRUE(param.is_valid());
  EXPECT_FALSE(param.get_lead_cost().is_valid());
}

TEST_F(ObDASMatchQueryTest, test_bmm_param_with_zero_lead_cost)
{
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ObSEArray<ObDASSearchCost, 1> score_op_costs;
  const ObDASSearchCost lead_cost(0);
  ObDASBMMBuildOptions param_options;
  param_options.lead_cost_ = lead_cost;
  ObDASBMMOpParam param(score_ops, nullptr, allocator_, param_options, &score_op_costs);

  ASSERT_TRUE(param.get_lead_cost().is_valid());
  EXPECT_EQ(0, param.get_lead_cost().cost());
}

TEST_F(ObDASMatchQueryTest, test_generate_op_empty_query_tokens)
{
  set_query_text("");

  SearchCtxSetup ctx_setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, op->get_op_type());
}

TEST_F(ObDASMatchQueryTest, test_query_string_generate_op_uses_empty_groups)
{
  set_query_text("");
  query_string_rtdef_.query_groups_prepared_ = true;
  query_string_rtdef_.token_count_ = 1;

  SearchCtxSetup ctx_setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      query_string_rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, op->get_op_type());
}

TEST_F(ObDASMatchQueryTest, test_generate_async_bmm_prepares_cold_token_costs)
{
  static const uint64_t INDEX_TABLE_ID = 9001;
  ir_scan_ctdef_.is_async_index_ = true;
  inv_idx_scan_ctdef_.is_async_index_ = true;
  inv_idx_scan_ctdef_.ref_table_id_ = INDEX_TABLE_ID;
  inv_idx_scan_ctdef_.table_param_.set_is_partition_table(false);
  match_ctdef_.set_is_scoring(true);
  match_ctdef_.set_is_top_level_scoring(true);
  set_query_text("ocean base");
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(0);

  SearchCtxSetup ctx_setup(allocator_);
  ctx_setup.add_related_scan(inv_idx_scan_ctdef_, inv_idx_scan_rtdef_, ObTabletID(0));
  ctx_setup.search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObInvIdxParamCache &cache = ctx_setup.search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS,
      cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS,
      cache.set_token_doc_cnt(ObTabletID(0), ObString::make_string("base"), 50));
  ObDASAsyncIndexQueryCtx catalog;
  build_single_segment_catalog(catalog, ctx_setup.search_ctx_, INDEX_TABLE_ID);

  EXPECT_FALSE(match_rtdef_.cost_.is_valid());
  EXPECT_FALSE(match_rtdef_.query_tokens_prepared_);
  EXPECT_TRUE(match_rtdef_.query_tokens_.empty());
  EXPECT_TRUE(match_rtdef_.token_costs_.empty());

  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      match_rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_BMM, op->get_op_type());
  EXPECT_TRUE(match_rtdef_.cost_.is_valid());
  EXPECT_TRUE(match_rtdef_.query_tokens_prepared_);
  ASSERT_EQ(2, match_rtdef_.query_tokens_.count());
  ASSERT_EQ(match_rtdef_.query_tokens_.count(), match_rtdef_.token_costs_.count());
  for (int64_t i = 0; i < match_rtdef_.query_tokens_.count(); ++i) {
    const ObString &token = match_rtdef_.query_tokens_.at(i);
    const int64_t expected_cost = token == ObString::make_string("ocean") ? 100
        : token == ObString::make_string("base") ? 50
        : -1;
    EXPECT_NE(-1, expected_cost);
    EXPECT_TRUE(match_rtdef_.token_costs_.at(i).is_valid());
    EXPECT_EQ(expected_cost, match_rtdef_.token_costs_.at(i).cost());
  }
}

// 无 tablet/t3m 的 UT 不能构造真实 ObDASTokenOp（sync 路径会在
// init_scan_range 解引用空的 search_text_）。非空 token 的算子树选型覆盖
// 放到 mittest / 完整 storage 环境；此处只验证 prepare 缓存与 empty→Dummy 路径。
TEST_F(ObDASMatchQueryTest, test_generate_op_reuses_prepared_tokens)
{
  set_query_text("ocean base");
  ASSERT_FALSE(match_rtdef_.query_tokens_prepared_);

  ASSERT_EQ(OB_SUCCESS, match_rtdef_.prepare_query_tokens());
  EXPECT_TRUE(match_rtdef_.query_tokens_prepared_);
  EXPECT_GT(match_rtdef_.query_tokens_.count(), 0);

  // 清空 tokens 但保留 prepared 标记，避免构造真实 TokenOp；
  // generate_op 应走 empty→Dummy，且不会重新 tokenize。
  match_rtdef_.query_tokens_.reuse();
  SearchCtxSetup ctx_setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, op->get_op_type());
  EXPECT_TRUE(match_rtdef_.query_tokens_prepared_);
  EXPECT_EQ(0, match_rtdef_.query_tokens_.count());
}

// ============================================================================
// Section 5: can_pushdown_filter_to_bmm Tests
// ============================================================================

TEST_F(ObDASMatchQueryTest, test_can_pushdown_filter_to_bmm)
{
  bool can_pushdown = false;
  match_ctdef_.set_is_top_level_scoring(true);
  set_match_operator(ObMatchOperator::MATCH_OPERATOR_OR);
  set_minimum_should_match(1);

  SearchCtxSetup ctx_setup(allocator_);
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.can_pushdown_filter_to_bmm(ctx_setup.search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);

  set_match_operator(ObMatchOperator::MATCH_OPERATOR_AND);
  ASSERT_EQ(OB_SUCCESS, match_rtdef_.can_pushdown_filter_to_bmm(ctx_setup.search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  if (OB_ISNULL(GCTX.plugin_mgr_)) {
    GCTX.plugin_mgr_ = OB_NEW(
        oceanbase::plugin::ObPluginMgr, ObMemAttr(OB_SYS_TENANT_ID, "TestMatchQ"));
    if (OB_ISNULL(GCTX.plugin_mgr_)
        || OB_SUCCESS != GCTX.plugin_mgr_->init(ObString(""))
        || OB_SUCCESS != GCTX.plugin_mgr_->load_builtin_plugins()) {
      return OB_ERR_UNEXPECTED;
    }
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

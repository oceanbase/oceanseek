/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information.
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#ifdef CONST
#undef CONST
#endif
#include "lib/allocator/ob_allocator.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_async_scalar_range_op.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/engine/ob_exec_context.h"
#undef protected
#undef private

using namespace oceanbase;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

namespace
{

static const uint64_t RUNTIME_INDEX_TABLE_ID = 9101;
static const ObTabletID RUNTIME_INDEX_TABLET_ID(19101);

class TestDASAsyncScalarRangeRuntime : public ::testing::Test
{
public:
  TestDASAsyncScalarRangeRuntime()
    : allocator_("AsyncRangeRT"), scan_allocator_("AsyncRangeScan"),
      scan_op_(scan_allocator_), exec_ctx_(allocator_), eval_ctx_(exec_ctx_),
      rowid_exprs_(allocator_), output_(allocator_), search_ctx_(allocator_, scan_op_)
  {}

  void SetUp() override
  {
    ObTenantEnv::get_tenant_local()->id_ = 500;
    eval_ctx_.max_batch_size_ = 8;
    search_ctx_.eval_ctx_ = &eval_ctx_;
    search_ctx_.rowid_exprs_ = &rowid_exprs_;
    search_ctx_.output_ = &output_;
    search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs_;
    search_ctx_.set_ls_id(ObLSID(1001));
  }

  int init_metadata(ObDASScalarScanCtDef &ctdef,
                    ObDASScalarScanRtDef &rtdef,
                    ObExpr &rowid_expr)
  {
    ctdef.ref_table_id_ = RUNTIME_INDEX_TABLE_ID;
    ctdef.is_async_index_ = true;
    ctdef.enable_new_false_range_ = true;
    int ret = ctdef.rowkey_exprs_.init(1);
    if (OB_SUCC(ret)) {
      ret = ctdef.rowkey_exprs_.push_back(&rowid_expr);
    }
    if (OB_SUCC(ret)) {
      ret = rowid_exprs_.init(1);
    }
    if (OB_SUCC(ret)) {
      ret = rowid_exprs_.push_back(&rowid_expr);
    }
    if (OB_SUCC(ret)) {
      rtdef.ctdef_ = &ctdef;
    }
    return ret;
  }

  int build_partition(ObAsyncIndexPartitionSegCtx &partition,
                      const ObLSID &ls_id,
                      const uint64_t index_table_id,
                      const ObTabletID &index_tablet_id,
                      const ObNewVersionRange &range)
  {
    int ret = OB_SUCCESS;
    using SegmentGroup = storage::ObAsyncIndexSegmentGroup;
    SegmentGroup *group = nullptr;
    partition.ls_id_ = ls_id;
    if (OB_FAIL(partition.index_table_ids_.push_back(index_table_id))) {
    } else if (OB_FAIL(partition.index_tablet_ids_.push_back(index_tablet_id))) {
    } else if (OB_ISNULL(group = OB_NEWx(
                   storage::ObAsyncIndexSegmentGroup,
                   &partition.storage_group_ctx_.allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(group->init(1))) {
    }
    if (OB_SUCC(ret) && OB_FAIL(group->add_version_range(0, range))) {
    }
    if (OB_SUCC(ret)
        && OB_FAIL(partition.storage_group_ctx_.groups_.push_back(group))) {
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(group)) {
      group->reset();
      OB_DELETEx(SegmentGroup, &partition.storage_group_ctx_.allocator_, group);
    }
    return ret;
  }

protected:
  ObArenaAllocator allocator_;
  ObArenaAllocator scan_allocator_;
  ObDASScanOp scan_op_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ExprFixedArray rowid_exprs_;
  ExprFixedArray output_;
  ObDASSearchCtx search_ctx_;
};

void set_term(ObObj *objs, const int64_t first, const int64_t second, const int64_t third)
{
  objs[0].set_int(first);
  objs[1].set_int(second);
  objs[2].set_int(third);
}

void set_range(ObNewRange &range, ObObj *start_objs, ObObj *end_objs, const int64_t count)
{
  range.start_key_.assign(start_objs, count);
  range.end_key_.assign(end_objs, count);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
}

TEST(TestDASAsyncScalarRange, project_closed_term_range)
{
  ObArenaAllocator allocator;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDatumRange term_range;
  set_term(start_objs, 1, 2, 3);
  set_term(end_objs, 4, 5, 6);
  start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT);

  ASSERT_EQ(OB_SUCCESS, ObDASAsyncScalarRangeOp::project_term_range(
      range, allocator, term_range));
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.get_start_key().get_datum_cnt());
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.get_end_key().get_datum_cnt());
  EXPECT_EQ(1, term_range.get_start_key().get_datum(0).get_int());
  EXPECT_EQ(2, term_range.get_start_key().get_datum(1).get_int());
  EXPECT_EQ(3, term_range.get_start_key().get_datum(2).get_int());
  EXPECT_TRUE(term_range.get_start_key().get_datum(SEARCH_INDEX_DOC_ID).is_min());
  EXPECT_EQ(4, term_range.get_end_key().get_datum(0).get_int());
  EXPECT_EQ(5, term_range.get_end_key().get_datum(1).get_int());
  EXPECT_EQ(6, term_range.get_end_key().get_datum(2).get_int());
  EXPECT_TRUE(term_range.get_end_key().get_datum(SEARCH_INDEX_DOC_ID).is_max());
  EXPECT_TRUE(term_range.get_border_flag().inclusive_start());
  EXPECT_TRUE(term_range.get_border_flag().inclusive_end());
}

void overwrite_projection_stack()
{
  volatile unsigned char stack_buf[8192];
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(stack_buf)); ++i) {
    stack_buf[i] = 0xA5;
  }
}

TEST(TestDASAsyncScalarRange, project_term_range_store_rowkey_outlives_projection)
{
  ObArenaAllocator allocator;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDatumRange term_range;
  set_term(start_objs, 1, 2, 3);
  set_term(end_objs, 4, 5, 6);
  start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT);

  ASSERT_EQ(OB_SUCCESS, ObDASAsyncScalarRangeOp::project_term_range(
      range, allocator, term_range));
  overwrite_projection_stack();

  const ObStoreRowkey &start_key = term_range.get_start_key().get_store_rowkey();
  const ObStoreRowkey &end_key = term_range.get_end_key().get_store_rowkey();
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, start_key.get_obj_cnt());
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, end_key.get_obj_cnt());
  ASSERT_NE(start_objs, start_key.get_obj_ptr());
  ASSERT_NE(end_objs, end_key.get_obj_ptr());
  EXPECT_EQ(1, start_key.get_obj_ptr()[0].get_int());
  EXPECT_EQ(2, start_key.get_obj_ptr()[1].get_int());
  EXPECT_EQ(3, start_key.get_obj_ptr()[2].get_int());
  EXPECT_TRUE(start_key.get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_min_value());
  EXPECT_EQ(4, end_key.get_obj_ptr()[0].get_int());
  EXPECT_EQ(5, end_key.get_obj_ptr()[1].get_int());
  EXPECT_EQ(6, end_key.get_obj_ptr()[2].get_int());
  EXPECT_TRUE(end_key.get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_max_value());
}

TEST(TestDASAsyncScalarRange, project_open_term_range)
{
  ObArenaAllocator allocator;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDatumRange term_range;
  set_term(start_objs, 1, 2, 3);
  set_term(end_objs, 4, 5, 6);
  start_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT);

  ASSERT_EQ(OB_SUCCESS, ObDASAsyncScalarRangeOp::project_term_range(
      range, allocator, term_range));
  EXPECT_EQ(SEARCH_INDEX_DOC_ID, term_range.get_start_key().get_datum_cnt());
  EXPECT_EQ(SEARCH_INDEX_DOC_ID, term_range.get_end_key().get_datum_cnt());
  EXPECT_FALSE(term_range.get_border_flag().inclusive_start());
  EXPECT_FALSE(term_range.get_border_flag().inclusive_end());
}

TEST(TestDASAsyncScalarRange, project_mixed_physical_boundaries)
{
  ObArenaAllocator allocator;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDatumRange term_range;

  set_term(end_objs, 4, 5, 6);
  end_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  range.start_key_.set_min_row();
  range.end_key_.assign(end_objs, SEARCH_INDEX_KEY_CNT);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, ObDASAsyncScalarRangeOp::project_term_range(
      range, allocator, term_range));
  EXPECT_TRUE(term_range.get_start_key().is_min_rowkey());
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.get_end_key().get_datum_cnt());
  EXPECT_TRUE(term_range.get_end_key().get_datum(SEARCH_INDEX_DOC_ID).is_max());

  term_range.reset();
  allocator.reuse();
  range.reset();
  set_term(start_objs, 1, 2, 3);
  start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  range.start_key_.assign(start_objs, SEARCH_INDEX_KEY_CNT);
  range.end_key_.set_max_row();
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, ObDASAsyncScalarRangeOp::project_term_range(
      range, allocator, term_range));
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.get_start_key().get_datum_cnt());
  EXPECT_TRUE(term_range.get_start_key().get_datum(SEARCH_INDEX_DOC_ID).is_min());
  EXPECT_TRUE(term_range.get_end_key().is_max_rowkey());
}

TEST(TestDASAsyncScalarRange, project_open_term_range_for_estimate)
{
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDASSearchUtils::AsyncScalarTermRange term_range;
  set_term(start_objs, 1, 2, 3);
  set_term(end_objs, 4, 5, 6);
  start_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT);

  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::project_async_scalar_term_range(
      range, term_range));
  ASSERT_EQ(SEARCH_INDEX_DOC_ID, term_range.range_.get_start_key().get_obj_cnt());
  ASSERT_EQ(SEARCH_INDEX_DOC_ID, term_range.range_.get_end_key().get_obj_cnt());
  EXPECT_EQ(1, term_range.range_.get_start_key().get_obj_ptr()[0].get_int());
  EXPECT_EQ(2, term_range.range_.get_start_key().get_obj_ptr()[1].get_int());
  EXPECT_EQ(3, term_range.range_.get_start_key().get_obj_ptr()[2].get_int());
  EXPECT_EQ(4, term_range.range_.get_end_key().get_obj_ptr()[0].get_int());
  EXPECT_EQ(5, term_range.range_.get_end_key().get_obj_ptr()[1].get_int());
  EXPECT_EQ(6, term_range.range_.get_end_key().get_obj_ptr()[2].get_int());
  EXPECT_FALSE(term_range.range_.border_flag_.inclusive_start());
  EXPECT_FALSE(term_range.range_.border_flag_.inclusive_end());
}

TEST(TestDASAsyncScalarRange, project_closed_single_term_range_for_estimate)
{
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDASSearchUtils::AsyncScalarTermRange term_range;
  set_term(start_objs, 1, 2, 3);
  set_term(end_objs, 1, 2, 3);
  start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT);

  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::project_async_scalar_term_range(
      range, term_range));
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.range_.get_start_key().get_obj_cnt());
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.range_.get_end_key().get_obj_cnt());
  EXPECT_TRUE(term_range.range_.get_start_key().get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_min_value());
  EXPECT_TRUE(term_range.range_.get_end_key().get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_max_value());
}

TEST(TestDASAsyncScalarRange, project_single_sided_closed_boundaries_for_estimate)
{
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDASSearchUtils::AsyncScalarTermRange term_range;

  set_term(end_objs, 4, 5, 6);
  end_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  range.start_key_.set_min_row();
  range.end_key_.assign(end_objs, SEARCH_INDEX_KEY_CNT);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::project_async_scalar_term_range(
      range, term_range));
  EXPECT_TRUE(term_range.range_.start_key_.is_min_row());
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.range_.end_key_.get_obj_cnt());
  EXPECT_TRUE(term_range.range_.end_key_.get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_max_value());

  range.reset();
  set_term(start_objs, 1, 2, 3);
  start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  range.start_key_.assign(start_objs, SEARCH_INDEX_KEY_CNT);
  range.end_key_.set_max_row();
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::project_async_scalar_term_range(
      range, term_range));
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.range_.start_key_.get_obj_cnt());
  EXPECT_TRUE(term_range.range_.start_key_.get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_min_value());
  EXPECT_TRUE(term_range.range_.end_key_.is_max_row());
}

TEST(TestDASAsyncScalarRange, project_mixed_open_closed_boundaries_for_estimate)
{
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDASSearchUtils::AsyncScalarTermRange term_range;

  set_term(start_objs, 1, 2, 3);
  set_term(end_objs, 4, 5, 6);
  start_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT);
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::project_async_scalar_term_range(
      range, term_range));
  EXPECT_EQ(SEARCH_INDEX_DOC_ID, term_range.range_.start_key_.get_obj_cnt());
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.range_.end_key_.get_obj_cnt());
  EXPECT_TRUE(term_range.range_.end_key_.get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_max_value());
  EXPECT_FALSE(term_range.range_.border_flag_.inclusive_start());
  EXPECT_TRUE(term_range.range_.border_flag_.inclusive_end());

  start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT);
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::project_async_scalar_term_range(
      range, term_range));
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, term_range.range_.start_key_.get_obj_cnt());
  EXPECT_EQ(SEARCH_INDEX_DOC_ID, term_range.range_.end_key_.get_obj_cnt());
  EXPECT_TRUE(term_range.range_.start_key_.get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_min_value());
  EXPECT_TRUE(term_range.range_.border_flag_.inclusive_start());
  EXPECT_FALSE(term_range.range_.border_flag_.inclusive_end());
}

TEST(TestDASAsyncScalarRange, project_whole_range)
{
  ObArenaAllocator allocator;
  ObNewRange range;
  ObDatumRange term_range;
  range.set_whole_range();

  ASSERT_EQ(OB_SUCCESS, ObDASAsyncScalarRangeOp::project_term_range(
      range, allocator, term_range));
  EXPECT_TRUE(term_range.is_whole_range());
}

TEST(TestDASAsyncScalarRange, reject_doc_id_bounds)
{
  ObArenaAllocator allocator;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDatumRange term_range;
  set_term(start_objs, 1, 2, 3);
  set_term(end_objs, 4, 5, 6);
  start_objs[SEARCH_INDEX_DOC_ID].set_uint64(10);
  end_objs[SEARCH_INDEX_DOC_ID].set_uint64(20);
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT);
  EXPECT_EQ(OB_NOT_SUPPORTED, ObDASAsyncScalarRangeOp::project_term_range(
      range, allocator, term_range));

  start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT);
  EXPECT_EQ(OB_SUCCESS, ObDASAsyncScalarRangeOp::project_term_range(
      range, allocator, term_range));
}

TEST(TestDASAsyncScalarRange, reject_five_column_range)
{
  ObArenaAllocator allocator;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT_WITH_NK];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT_WITH_NK];
  ObNewRange range;
  ObDatumRange term_range;
  set_term(start_objs, 1, 2, 3);
  set_term(end_objs, 4, 5, 6);
  start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_max_value();
  start_objs[SEARCH_INDEX_NESTED_KEY].set_min_value();
  end_objs[SEARCH_INDEX_NESTED_KEY].set_max_value();
  set_range(range, start_objs, end_objs, SEARCH_INDEX_KEY_CNT_WITH_NK);

  EXPECT_EQ(OB_NOT_SUPPORTED, ObDASAsyncScalarRangeOp::project_term_range(
      range, allocator, term_range));
}

TEST_F(TestDASAsyncScalarRangeRuntime,
       batch_wrapper_compatibility_and_empty_range_metrics)
{
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_metadata(ctdef, rtdef, rowid_expr));
  ObDASAsyncScalarRangeOpParam param(
      &ctdef, &rtdef, RUNTIME_INDEX_TABLE_ID, -1, false);
  ObDASAsyncScalarRangeOp range(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, range.init(param));

  ObOpProfile<> root_profile(ObProfileId::HYBRID_SEARCH_BITMAP, &allocator_);
  {
    ObProfileSwitcher switcher(&root_profile);
    ASSERT_EQ(OB_SUCCESS, range.open_profile());
  }
  ASSERT_NE(nullptr, range.profile_);

  int64_t count = 99;
  EXPECT_EQ(OB_ITER_END, range.next_rowids(4, count));
  EXPECT_EQ(0, count);
  count = 99;
  EXPECT_EQ(OB_ITER_END, range.get_next_rows(count, 4));
  EXPECT_EQ(0, count);
  bool exist = false;
  uint64_t value = 0;
  range.profile_->get_metric_value(ObMetricId::HS_NEXT_ROWIDS_COUNT, exist, value);
  EXPECT_TRUE(exist);
  EXPECT_EQ(2, value);
  EXPECT_EQ(nullptr, range.range_iter_);
  range.close_profile();
}

TEST_F(TestDASAsyncScalarRangeRuntime,
       direct_param_rejects_invalid_binding)
{
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_metadata(ctdef, rtdef, rowid_expr));

  ObDASAsyncScalarRangeOp *op = reinterpret_cast<ObDASAsyncScalarRangeOp *>(1);
  ObDASAsyncScalarRangeOpParam null_ctdef(
      nullptr, &rtdef, RUNTIME_INDEX_TABLE_ID, -1, false);
  EXPECT_EQ(OB_INVALID_ARGUMENT, search_ctx_.create_op(null_ctdef, op));
  EXPECT_EQ(nullptr, op);
  ObDASAsyncScalarRangeOpParam null_rtdef(
      &ctdef, nullptr, RUNTIME_INDEX_TABLE_ID, -1, false);
  EXPECT_EQ(OB_INVALID_ARGUMENT, search_ctx_.create_op(null_rtdef, op));
  EXPECT_EQ(nullptr, op);

  ObDASAsyncScalarRangeOpParam invalid_table(
      &ctdef, &rtdef, OB_INVALID_ID, 0, true);
  EXPECT_EQ(OB_INVALID_ARGUMENT, search_ctx_.create_op(invalid_table, op));
  EXPECT_EQ(nullptr, op);
  ObDASAsyncScalarRangeOpParam invalid_direct(
      &ctdef, &rtdef, RUNTIME_INDEX_TABLE_ID, -1, true);
  EXPECT_EQ(OB_INVALID_ARGUMENT, search_ctx_.create_op(invalid_direct, op));
  EXPECT_EQ(nullptr, op);
}

TEST_F(TestDASAsyncScalarRangeRuntime,
       direct_resolver_and_range_segment_ownership)
{
  ObNewVersionRange valid_a;
  ObNewVersionRange valid_b;
  valid_a.base_version_ = 10;
  valid_a.snapshot_version_ = 20;
  valid_b.base_version_ = 30;
  valid_b.snapshot_version_ = 40;
  ASSERT_TRUE(valid_a.is_valid());
  ASSERT_TRUE(valid_b.is_valid());

  ObAsyncIndexPartitionSegCtx valid_partition_a;
  ASSERT_EQ(OB_SUCCESS, build_partition(
      valid_partition_a, ObLSID(1001), RUNTIME_INDEX_TABLE_ID,
      RUNTIME_INDEX_TABLET_ID, valid_a));
  search_ctx_.set_current_async_partition(&valid_partition_a);

  ObNewVersionRange output;
  bool is_empty = false;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.resolve_direct_async_index_segment(
      RUNTIME_INDEX_TABLE_ID, RUNTIME_INDEX_TABLET_ID, 0, output, is_empty));
  EXPECT_EQ(valid_a.base_version_, output.base_version_);
  EXPECT_EQ(valid_a.snapshot_version_, output.snapshot_version_);

  ObAsyncIndexPartitionSegCtx valid_partition_b;
  ASSERT_EQ(OB_SUCCESS, build_partition(
      valid_partition_b, ObLSID(1001), RUNTIME_INDEX_TABLE_ID,
      RUNTIME_INDEX_TABLET_ID, valid_b));
  search_ctx_.set_current_async_partition(&valid_partition_b);
  ASSERT_EQ(OB_SUCCESS, search_ctx_.resolve_direct_async_index_segment(
      RUNTIME_INDEX_TABLE_ID, RUNTIME_INDEX_TABLET_ID, 0, output, is_empty));
  EXPECT_EQ(valid_b.base_version_, output.base_version_);
  EXPECT_EQ(valid_b.snapshot_version_, output.snapshot_version_);

  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_metadata(ctdef, rtdef, rowid_expr));
  ObDASAsyncScalarRangeOpParam direct_param(
      &ctdef, &rtdef, RUNTIME_INDEX_TABLE_ID, 0, true);
  ObDASAsyncScalarRangeOp direct(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, direct.init(direct_param));
  EXPECT_EQ(OB_STATE_NOT_MATCH, direct.rescan_segment(valid_b));
  ObDASAsyncScalarRangeOpParam lazy_param(
      &ctdef, &rtdef, RUNTIME_INDEX_TABLE_ID, -1, false);
  ObDASAsyncScalarRangeOp lazy(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, lazy.init(lazy_param));
  EXPECT_EQ(OB_SUCCESS, lazy.rescan_segment(valid_b));
  EXPECT_EQ(valid_b.base_version_, lazy.version_range_.base_version_);
}

} // namespace

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

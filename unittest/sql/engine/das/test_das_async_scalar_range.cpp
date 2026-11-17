/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information.
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#define private public
#define protected public
#include "common/ob_tenant_data_version_mgr.h"
#include "share/rc/ob_tenant_base.h"
#ifdef CONST
#undef CONST
#endif
#include "lib/allocator/ob_allocator.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_async_scalar_range_op.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "storage/tx_storage/ob_access_service.h"
#include "lib/json_type/ob_json_tree.h"
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

TEST_F(TestDASAsyncScalarRangeRuntime, direct_sort_bottom_preserves_binding_and_empty_intersection)
{
  struct MockVersionGuard
  {
    uint64_t saved_;
    ~MockVersionGuard() { ODV_MGR.set_mock_data_version(saved_); }
  } version_guard{ATOMIC_LOAD(&ODV_MGR.mock_data_version_)};
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_3_5_0);
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_metadata(ctdef, rtdef, rowid_expr));
  ObDASHybridSearchSortCtDef sort(allocator_);
  ObExpr price;
  price.obj_meta_.set_type(ObIntType);
  price.datum_meta_.type_ = ObIntType;
  sort.das_sort_topn_enable_ = true;
  sort.has_sort_value_lookup_ = true;
  ASSERT_EQ(OB_SUCCESS, sort.sort_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort.sort_exprs_.push_back(&price));
  ASSERT_EQ(OB_SUCCESS, sort.sort_collations_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort.sort_collations_.push_back(ObSortFieldCollation(0, CS_TYPE_BINARY, true, NULL_LAST)));
  ASSERT_EQ(OB_SUCCESS, sort.score_sort_flags_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort.score_sort_flags_.push_back(false));
  ASSERT_EQ(OB_SUCCESS, sort.unique_shard_doc_flags_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort.unique_shard_doc_flags_.push_back(false));
  auto &scan = sort.sort_value_scan_ctdef_;
  ASSERT_EQ(OB_SUCCESS, scan.pd_expr_spec_.access_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan.pd_expr_spec_.access_exprs_.push_back(&price));
  ASSERT_EQ(OB_SUCCESS, scan.access_column_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan.access_column_ids_.push_back(17));
  ObSEArray<ObColDesc, 2> columns;
  ObColDesc column;
  column.col_id_ = 16;
  column.col_type_.set_type(ObUInt64Type);
  column.col_order_ = ObOrderType::ASC;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(column));
  column.col_id_ = 17;
  column.col_type_.set_type(ObIntType);
  ASSERT_EQ(OB_SUCCESS, columns.push_back(column));
  ASSERT_EQ(OB_SUCCESS, scan.table_param_.main_read_info_.init(allocator_, 2, 1, false, columns, nullptr));
  search_ctx_.sort_info_.sort_ctdef_ = &sort;

  ObObj start[4];
  ObObj end[4];
  const int64_t ordinal = scan.table_param_.get_read_info().get_memtable_columns_index().at(1);
  start[0].set_int(ordinal);
  end[0].set_int(ordinal);
  start[1].set_null();
  end[1].set_null();
  int64_t number = 50;
  ObDatum value;
  value.ptr_ = reinterpret_cast<const char *>(&number);
  value.set_int(number);
  ObString encoded;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_value(allocator_, value, price.obj_meta_, encoded));
  start[2].set_varchar(encoded);
  start[2].set_collation_type(CS_TYPE_BINARY);
  end[2].set_max_value();
  start[3].set_min_value();
  end[3].set_max_value();
  ObNewRange original;
  set_range(original, start, end, 4);
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(original));
  ObDASAsyncScalarRangeOpParam param(&ctdef, &rtdef, RUNTIME_INDEX_TABLE_ID, 0, true);
  ObDASAsyncScalarRangeOp range(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, range.init(param));
  range.version_range_.base_version_ = 10;
  range.version_range_.snapshot_version_ = 20;
  range.inv_idx_tablet_id_ = RUNTIME_INDEX_TABLET_ID;
  number = 30; // [50, +inf] intersected with the ASC bottom <=30 is empty.
  bool applied = true;
  range.read_started_ = true;
  ASSERT_EQ(OB_SUCCESS, range.try_apply_sort_bottom(value, applied));
  EXPECT_FALSE(applied);
  range.read_started_ = false;
  ASSERT_EQ(OB_SUCCESS, range.try_apply_sort_bottom(value, applied));
  EXPECT_TRUE(applied);
  EXPECT_TRUE(range.direct_bind_);
  EXPECT_TRUE(range.has_sort_ranges_);
  EXPECT_TRUE(range.active_ranges().empty());
  EXPECT_EQ(10, range.version_range_.base_version_);
  EXPECT_EQ(20, range.version_range_.snapshot_version_);
  EXPECT_EQ(RUNTIME_INDEX_TABLET_ID, range.inv_idx_tablet_id_);
  EXPECT_EQ(1, rtdef.key_ranges_.count()); // The original query ranges are untouched.
  EXPECT_EQ(nullptr, range.range_iter_); // Empty intersection opens no storage iterator.
}

// Nonempty range intersections reopen an iterator, but these cases never read storage.
class MockSortBottomRangeAccessService : public storage::ObAccessService
{
public:
  MockSortBottomRangeAccessService() : open_count_(0) {}

  int async_index_range_scan(
      const ObLSID &ls_id,
      const storage::ObTabletHandle *pinned_tablet_handle,
      const ObNewVersionRange &version_range,
      ObTableScanParam &scan_param,
      ObInvRangeScanParam &range_param,
      ObInvSSTableRangeIter *&iter) override
  {
    UNUSEDx(ls_id, pinned_tablet_handle, version_range, scan_param);
    iter = OB_NEWx(ObInvSSTableRangeIter, range_param.allocator_);
    if (OB_NOT_NULL(iter)) {
      ++open_count_;
    }
    return OB_ISNULL(iter) ? OB_ALLOCATE_MEMORY_FAILED : OB_SUCCESS;
  }

  int64_t open_count_;
};

int make_sort_json_datum(ObIAllocator &allocator, ObIJsonBase *node, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *bin = nullptr;
  ObString binary;
  if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, node, ObJsonInType::JSON_BIN, bin))) {
  } else if (OB_FAIL(bin->get_raw_binary(binary, &allocator))) {
  } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(ObJsonType, allocator, binary, datum))) {
  }
  return ret;
}

enum class SortBottomWrapper { BARE, JSON_NULL, SQL_NULL, JSON_VALUE, NON_CONST, COALESCE };
enum class SortBottomRanges { ABOVE_BOTTOM, UPPER_BOUNDED, MIXED_SQL_NULL, SEPARATE_JSON_NULLS, CROSS_JSON_TYPES };

struct SortBottomPathCase
{
  SortBottomPathCase(ObObjType type, const char *bottom, const char *lower,
                    const char *range_member, bool binary_path, bool same_column, bool expected_applied,
                    SortBottomWrapper wrapper = SortBottomWrapper::BARE,
                    SortBottomRanges ranges = SortBottomRanges::ABOVE_BOTTOM,
                    bool ascending = true, ObCmpNullPos null_pos = NULL_LAST)
    : type_(type), bottom_(bottom), lower_(lower), range_member_(range_member),
      binary_path_(binary_path), same_column_(same_column), expected_applied_(expected_applied),
      wrapper_(wrapper), ranges_(ranges), ascending_(ascending), null_pos_(null_pos)
  {}
  ObObjType type_;
  const char *bottom_;
  const char *lower_;
  const char *range_member_;
  bool binary_path_;
  bool same_column_;
  bool expected_applied_;
  SortBottomWrapper wrapper_;
  SortBottomRanges ranges_;
  bool ascending_;
  ObCmpNullPos null_pos_;
};

class TestDASSortBottomPath
  : public TestDASAsyncScalarRangeRuntime,
    public ::testing::WithParamInterface<SortBottomPathCase>
{};

// Exercise actual range installation, including nonempty intersections and NULL
// ranges that must survive. Unsupported wrappers and prefixes leave the leaf unchanged.
TEST_P(TestDASSortBottomPath, ordinary_and_json_values_use_the_correct_path)
{
  const SortBottomPathCase &test_case = GetParam();
  struct MockVersionGuard
  {
    uint64_t saved_;
    ~MockVersionGuard() { ODV_MGR.set_mock_data_version(saved_); }
  } version_guard{ATOMIC_LOAD(&ODV_MGR.mock_data_version_)};
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_3_5_0);
  const bool json = test_case.type_ == ObJsonType;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_metadata(ctdef, rtdef, rowid_expr));

  ObExpr sort_expr;
  sort_expr.obj_meta_.set_type(test_case.type_);
  sort_expr.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  sort_expr.datum_meta_.type_ = test_case.type_;
  sort_expr.datum_meta_.cs_type_ = CS_TYPE_UTF8MB4_BIN;
  ObExpr json_column;
  ObExpr path_expr;
  ObExpr extract_expr;
  ObExpr null_expr;
  ObExpr *extract_args[] = {&json_column, &path_expr};
  ObExpr *wrapper_args[] = {&extract_expr, &null_expr};
  char *frames[2] = {nullptr, nullptr};
  for (int64_t i = 0; i < ARRAYSIZEOF(frames); ++i) {
    frames[i] = static_cast<char *>(allocator_.alloc(sizeof(ObDatum) + sizeof(ObEvalInfo)));
    ASSERT_NE(nullptr, frames[i]);
    MEMSET(frames[i], 0, sizeof(ObDatum) + sizeof(ObEvalInfo));
    new (frames[i]) ObDatum();
  }
  exec_ctx_.frames_ = frames;
  exec_ctx_.frame_cnt_ = ARRAYSIZEOF(frames);
  eval_ctx_.frames_ = frames;
  path_expr.frame_idx_ = 0;
  path_expr.datum_off_ = 0;
  path_expr.eval_info_off_ = sizeof(ObDatum);
  path_expr.is_static_const_ = true;
  path_expr.obj_meta_.set_varchar();
  path_expr.datum_meta_.type_ = ObVarcharType;
  reinterpret_cast<ObDatum *>(frames[0])->set_string(ObString::make_string("$.price"));
  if (json) {
    sort_expr.type_ = T_FUN_SYS_JSON_EXTRACT;
    sort_expr.arg_cnt_ = 2;
    sort_expr.args_ = extract_args;
    if (test_case.wrapper_ != SortBottomWrapper::BARE) {
      extract_expr.type_ = T_FUN_SYS_JSON_EXTRACT;
      extract_expr.arg_cnt_ = 2;
      extract_expr.args_ = extract_args;
      extract_expr.obj_meta_ = sort_expr.obj_meta_;
      extract_expr.datum_meta_ = sort_expr.datum_meta_;
      null_expr.frame_idx_ = 1;
      null_expr.datum_off_ = 0;
      null_expr.eval_info_off_ = sizeof(ObDatum);
      null_expr.is_static_const_ = test_case.wrapper_ != SortBottomWrapper::NON_CONST;
      null_expr.obj_meta_ = sort_expr.obj_meta_;
      null_expr.datum_meta_ = sort_expr.datum_meta_;
      ObDatum &rhs = *reinterpret_cast<ObDatum *>(frames[1]);
      if (test_case.wrapper_ == SortBottomWrapper::SQL_NULL) {
        rhs.set_null();
      } else {
        ObIJsonBase *node = nullptr;
        const char *literal = test_case.wrapper_ == SortBottomWrapper::JSON_VALUE ? "1" : "null";
        ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator_, ObString::make_string(literal),
            ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, node));
        ASSERT_EQ(OB_SUCCESS, make_sort_json_datum(allocator_, node, rhs));
      }
      sort_expr.type_ = test_case.wrapper_ == SortBottomWrapper::COALESCE
          ? T_FUN_SYS_COALESCE : T_FUN_SYS_NULLIF;
      sort_expr.args_ = wrapper_args;
    }
  }
  const ObItemType original_sort_type = sort_expr.type_;

  ObDASHybridSearchSortCtDef sort(allocator_);
  sort.das_sort_topn_enable_ = true;
  sort.has_sort_value_lookup_ = true;
  ASSERT_EQ(OB_SUCCESS, sort.sort_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort.sort_exprs_.push_back(&sort_expr));
  ASSERT_EQ(OB_SUCCESS, sort.sort_collations_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort.sort_collations_.push_back(ObSortFieldCollation(0, CS_TYPE_UTF8MB4_BIN,
      test_case.ascending_, test_case.null_pos_)));
  ASSERT_EQ(OB_SUCCESS, sort.score_sort_flags_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort.score_sort_flags_.push_back(false));
  ASSERT_EQ(OB_SUCCESS, sort.unique_shard_doc_flags_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort.unique_shard_doc_flags_.push_back(false));
  auto &scan = sort.sort_value_scan_ctdef_;
  ASSERT_EQ(OB_SUCCESS, scan.pd_expr_spec_.access_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan.pd_expr_spec_.access_exprs_.push_back(json ? &json_column : &sort_expr));
  ASSERT_EQ(OB_SUCCESS, scan.access_column_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan.access_column_ids_.push_back(17));
  ObSEArray<ObColDesc, 2> columns;
  ObColDesc column;
  column.col_id_ = 16;
  column.col_type_.set_type(ObUInt64Type);
  column.col_order_ = ObOrderType::ASC;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(column));
  column.col_id_ = 17;
  column.col_type_ = sort_expr.obj_meta_;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(column));
  ASSERT_EQ(OB_SUCCESS, scan.table_param_.main_read_info_.init(allocator_, 2, 1, false, columns, nullptr));
  search_ctx_.sort_info_.sort_ctdef_ = &sort;

  ObIJsonBase *nodes[2] = {nullptr, nullptr};
  const char *texts[] = {test_case.bottom_, test_case.lower_};
  ObDatum values[2];
  int64_t datum_storage[2][2] = {};
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator_, ObString::make_string(texts[i]),
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, nodes[i]));
    if (json) {
      ASSERT_EQ(OB_SUCCESS, make_sort_json_datum(allocator_, nodes[i], values[i]));
    } else {
      ObObj obj;
      if (test_case.type_ == ObIntType) {
        int64_t number = 0;
        ASSERT_EQ(OB_SUCCESS, nodes[i]->to_int(number, true));
        obj.set_int(number);
      } else if (test_case.type_ == ObDoubleType) {
        obj.set_double(nodes[i]->get_double());
      } else {
        obj.set_varchar(ObString(nodes[i]->get_data_length(), nodes[i]->get_data()));
        obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
      values[i].ptr_ = reinterpret_cast<const char *>(datum_storage[i]);
      ASSERT_EQ(OB_SUCCESS, values[i].from_obj(obj));
    }
  }
  ObObj start[SEARCH_INDEX_KEY_CNT];
  ObObj end[SEARCH_INDEX_KEY_CNT];
  const int64_t ordinal = scan.table_param_.get_read_info().get_memtable_columns_index().at(1);
  start[SEARCH_INDEX_COL_IDX].set_int(ordinal + (test_case.same_column_ ? 0 : 1));
  end[SEARCH_INDEX_COL_IDX] = start[SEARCH_INDEX_COL_IDX];
  if (json) {
    ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> items;
    ASSERT_EQ(OB_SUCCESS, items.push_back(ObSearchIndexPathEncoder::make_object_path(
        ObString::make_string(test_case.range_member_))));
    ObString path;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexPathEncoder::encode_path(allocator_, items, nodes[1]->json_type(), path));
    start[SEARCH_INDEX_PATH].set_varbinary(path);
    if (!test_case.binary_path_) {
      start[SEARCH_INDEX_PATH].set_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  } else {
    start[SEARCH_INDEX_PATH].set_null();
  }
  end[SEARCH_INDEX_PATH] = start[SEARCH_INDEX_PATH];
  ObString encoded_lower;
  ASSERT_EQ(OB_SUCCESS, json
      ? ObSearchIndexValueEncoder::encode_value(allocator_, nodes[1], encoded_lower)
      : ObSearchIndexValueEncoder::encode_value(allocator_, values[1], sort_expr.obj_meta_, encoded_lower));
  start[SEARCH_INDEX_VALUE].set_varbinary(encoded_lower);
  end[SEARCH_INDEX_VALUE].set_max_value();
  start[SEARCH_INDEX_DOC_ID].set_min_value();
  end[SEARCH_INDEX_DOC_ID].set_max_value();
  if (test_case.ranges_ == SortBottomRanges::UPPER_BOUNDED) {
    start[SEARCH_INDEX_VALUE].set_min_value();
    end[SEARCH_INDEX_VALUE].set_varbinary(encoded_lower);
  }
  ObString null_path;
  ObString null_value;
  if (test_case.ranges_ == SortBottomRanges::MIXED_SQL_NULL) {
    start[SEARCH_INDEX_VALUE].set_null();
  } else if (test_case.ranges_ == SortBottomRanges::SEPARATE_JSON_NULLS
             || test_case.ranges_ == SortBottomRanges::CROSS_JSON_TYPES) {
    ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> items;
    ASSERT_EQ(OB_SUCCESS, items.push_back(ObSearchIndexPathEncoder::make_object_path(
        ObString::make_string(test_case.range_member_))));
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexPathEncoder::encode_path(
        allocator_, items, ObJsonNodeType::J_NULL, null_path));
    ObIJsonBase *node = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator_, ObString::make_string("null"),
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, node));
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_value(allocator_, node, null_value));
    if (test_case.ranges_ == SortBottomRanges::CROSS_JSON_TYPES) {
      start[SEARCH_INDEX_PATH].set_varbinary(null_path);
      start[SEARCH_INDEX_VALUE].set_varbinary(null_value);
    }
  }
  ObNewRange original;
  original.table_id_ = RUNTIME_INDEX_TABLE_ID;
  set_range(original, start, end, SEARCH_INDEX_KEY_CNT);
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(original));
  ObObj null_starts[2][SEARCH_INDEX_KEY_CNT];
  ObObj null_ends[2][SEARCH_INDEX_KEY_CNT];
  if (test_case.ranges_ == SortBottomRanges::SEPARATE_JSON_NULLS) {
    for (int64_t i = 0; i < 2; ++i) {
      null_starts[i][SEARCH_INDEX_COL_IDX].set_int(ordinal);
      null_starts[i][SEARCH_INDEX_PATH].set_null();
      null_starts[i][SEARCH_INDEX_VALUE].set_null();
      if (i == 0) {
        null_starts[i][SEARCH_INDEX_PATH].set_varbinary(null_path);
        null_starts[i][SEARCH_INDEX_VALUE].set_varbinary(null_value);
      }
      for (int64_t j = 0; j < SEARCH_INDEX_DOC_ID; ++j) {
        null_ends[i][j] = null_starts[i][j];
      }
      null_starts[i][SEARCH_INDEX_DOC_ID].set_min_value();
      null_ends[i][SEARCH_INDEX_DOC_ID].set_max_value();
      ObNewRange null_range;
      null_range.table_id_ = RUNTIME_INDEX_TABLE_ID;
      set_range(null_range, null_starts[i], null_ends[i], SEARCH_INDEX_KEY_CNT);
      ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(null_range));
    }
  }
  ObSEArray<ObNewRange, 3> saved_ranges;
  for (int64_t i = 0; i < rtdef.key_ranges_.count(); ++i) {
    ObNewRange saved;
    ASSERT_EQ(OB_SUCCESS, deep_copy_range(allocator_, rtdef.key_ranges_.at(i), saved));
    ASSERT_EQ(OB_SUCCESS, saved_ranges.push_back(saved));
  }
  MockSortBottomRangeAccessService access_service;
  ObAsyncIndexPartitionSegCtx partition;
  ObDASAsyncScalarRangeOpParam param(&ctdef, &rtdef, RUNTIME_INDEX_TABLE_ID, 0, true);
  ObDASAsyncScalarRangeOp range(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, range.init(param));
  range.version_range_.base_version_ = 10;
  range.version_range_.snapshot_version_ = 20;
  range.inv_idx_tablet_id_ = RUNTIME_INDEX_TABLET_ID;
  range.tsc_service_ = &access_service;
  range.inv_idx_scan_param_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, build_partition(partition, search_ctx_.get_ls_id(),
      RUNTIME_INDEX_TABLE_ID, RUNTIME_INDEX_TABLET_ID, range.version_range_));
  search_ctx_.set_current_async_partition(&partition);
  bool applied = false;
  ASSERT_EQ(OB_SUCCESS, range.try_apply_sort_bottom(values[0], applied));
  EXPECT_EQ(test_case.expected_applied_, applied);
  EXPECT_EQ(test_case.expected_applied_, range.has_sort_ranges_);
  EXPECT_EQ(&sort_expr, sort.sort_exprs_.at(0));
  EXPECT_EQ(original_sort_type, sort_expr.type_);
  if (test_case.wrapper_ != SortBottomWrapper::BARE) {
    EXPECT_EQ(&extract_expr, sort_expr.args_[0]);
    EXPECT_EQ(&null_expr, sort_expr.args_[1]);
  }
  ASSERT_EQ(saved_ranges.count(), rtdef.key_ranges_.count());
  for (int64_t i = 0; i < saved_ranges.count(); ++i) {
    EXPECT_TRUE(saved_ranges.at(i).equal(rtdef.key_ranges_.at(i)));
  }
  const bool expected_empty = test_case.expected_applied_
      && test_case.ranges_ == SortBottomRanges::ABOVE_BOTTOM;
  ASSERT_EQ(expected_empty ? 0 : saved_ranges.count(), range.active_ranges().count());
  EXPECT_EQ(test_case.expected_applied_ && !expected_empty ? 1 : 0, access_service.open_count_);
  EXPECT_EQ(test_case.expected_applied_ && !expected_empty, OB_NOT_NULL(range.range_iter_));
  if (!test_case.expected_applied_) {
    EXPECT_EQ(0, range.sort_range_allocator_.total());
    for (int64_t i = 0; i < saved_ranges.count(); ++i) {
      EXPECT_TRUE(saved_ranges.at(i).equal(range.active_ranges().at(i)));
    }
  } else if (!expected_empty) {
    ObString encoded_bottom;
    ASSERT_EQ(OB_SUCCESS, json
        ? ObSearchIndexValueEncoder::encode_value(allocator_, nodes[0], encoded_bottom)
        : ObSearchIndexValueEncoder::encode_value(allocator_, values[0], sort_expr.obj_meta_, encoded_bottom));
    const ObNewRange &active = range.active_ranges().at(0);
    const ObRowkey &cut = test_case.ascending_ ? active.end_key_ : active.start_key_;
    ASSERT_EQ(SEARCH_INDEX_KEY_CNT, cut.get_obj_cnt());
    EXPECT_EQ(encoded_bottom, cut.get_obj_ptr()[SEARCH_INDEX_VALUE].get_string());
    EXPECT_TRUE(test_case.ascending_ ? cut.get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_max_value()
                                    : cut.get_obj_ptr()[SEARCH_INDEX_DOC_ID].is_min_value());
    EXPECT_TRUE(test_case.ascending_ ? active.border_flag_.inclusive_end()
                                    : active.border_flag_.inclusive_start());
    if (test_case.ranges_ == SortBottomRanges::MIXED_SQL_NULL && test_case.ascending_) {
      EXPECT_TRUE(active.start_key_.get_obj_ptr()[SEARCH_INDEX_VALUE].is_null());
      EXPECT_TRUE(active.border_flag_.inclusive_start());
    }
    // JSON null and whole-column SQL NULL have different physical paths; both survive unchanged.
    for (int64_t i = 1; i < saved_ranges.count(); ++i) {
      EXPECT_TRUE(saved_ranges.at(i).equal(range.active_ranges().at(i)));
    }
  }
  if (test_case.expected_applied_) {
    range.read_started_ = true;
    range.release_scan_resources();
    EXPECT_EQ(nullptr, range.range_iter_);
    EXPECT_TRUE(range.read_started_);
    EXPECT_TRUE(range.has_sort_ranges_);
    EXPECT_TRUE(range.active_ranges().empty());
    EXPECT_EQ(0, range.sort_range_allocator_.total());
    EXPECT_EQ(10, range.version_range_.base_version_);
    EXPECT_EQ(20, range.version_range_.snapshot_version_);
    EXPECT_EQ(RUNTIME_INDEX_TABLET_ID, range.inv_idx_tablet_id_);

    ASSERT_EQ(OB_SUCCESS, scan_op_.get_related_ctdefs().init(1));
    ASSERT_EQ(OB_SUCCESS, scan_op_.get_related_ctdefs().push_back(&ctdef));
    ASSERT_EQ(OB_SUCCESS, scan_op_.get_related_tablet_ids().init(1));
    ASSERT_EQ(OB_SUCCESS, scan_op_.get_related_tablet_ids().push_back(RUNTIME_INDEX_TABLET_ID));
    const int64_t open_count = access_service.open_count_;
    ASSERT_EQ(OB_SUCCESS, range.rescan());
    EXPECT_FALSE(range.read_started_);
    EXPECT_FALSE(range.has_sort_ranges_);
    EXPECT_EQ(open_count + 1, access_service.open_count_);
    EXPECT_NE(nullptr, range.range_iter_);
    ASSERT_EQ(saved_ranges.count(), range.active_ranges().count());
    for (int64_t i = 0; i < saved_ranges.count(); ++i) {
      EXPECT_TRUE(saved_ranges.at(i).equal(range.active_ranges().at(i)));
    }
  }
  range.release_scan_resources();
}

INSTANTIATE_TEST_CASE_P(ValueTypesAndPathIsolation, TestDASSortBottomPath, ::testing::Values(
    SortBottomPathCase{ObIntType, "30", "50", "price", true, true, true},
    SortBottomPathCase{ObDoubleType, "1.25", "2.5", "price", true, true, true},
    SortBottomPathCase{ObVarcharType, "\"b\"", "\"d\"", "price", true, true, true},
    SortBottomPathCase{ObJsonType, "-30", "-10", "price", true, true, true},
    SortBottomPathCase{ObJsonType, "9007199254740993", "9007199254740994", "price", true, true, true},
    SortBottomPathCase{ObJsonType, "1.25", "2.5", "price", true, true, true},
    SortBottomPathCase{ObJsonType, "\"b\"", "\"d\"", "price", true, true, true},
    SortBottomPathCase{ObJsonType, "false", "true", "price", true, true, true},
    SortBottomPathCase{ObJsonType, "30", "50", "stock", true, true, false},
    SortBottomPathCase{ObJsonType, "30", "\"50\"", "price", true, true, false},
    SortBottomPathCase{ObJsonType, "30", "50", "price", false, true, false},
    SortBottomPathCase{ObJsonType, "30", "50", "price", true, false, false},
    SortBottomPathCase{ObJsonType, "null", "50", "price", true, true, false},
    SortBottomPathCase{ObJsonType, "{}", "50", "price", true, true, false},
    SortBottomPathCase{ObJsonType, "30", "50", "price", true, true, true, SortBottomWrapper::JSON_NULL},
    SortBottomPathCase{ObJsonType, "30", "50", "price", true, true, false, SortBottomWrapper::SQL_NULL},
    SortBottomPathCase{ObJsonType, "30", "50", "price", true, true, false, SortBottomWrapper::JSON_VALUE},
    SortBottomPathCase{ObJsonType, "30", "50", "price", true, true, false, SortBottomWrapper::NON_CONST},
    SortBottomPathCase{ObJsonType, "30", "50", "price", true, true, false, SortBottomWrapper::COALESCE},
    SortBottomPathCase{ObIntType, "30", "30", "price", true, true, false, SortBottomWrapper::BARE,
        SortBottomRanges::UPPER_BOUNDED},
    SortBottomPathCase{ObIntType, "30", "20", "price", true, true, false, SortBottomWrapper::BARE,
        SortBottomRanges::UPPER_BOUNDED},
    SortBottomPathCase{ObJsonType, "30", "30", "price", true, true, false, SortBottomWrapper::JSON_NULL,
        SortBottomRanges::UPPER_BOUNDED},
    SortBottomPathCase{ObIntType, "30", "30", "price", true, true, false, SortBottomWrapper::BARE,
        SortBottomRanges::ABOVE_BOTTOM, false, NULL_FIRST},
    SortBottomPathCase{ObIntType, "30", "50", "price", true, true, false, SortBottomWrapper::BARE,
        SortBottomRanges::ABOVE_BOTTOM, false, NULL_FIRST},
    SortBottomPathCase{ObIntType, "30", "10", "price", true, true, true, SortBottomWrapper::BARE,
        SortBottomRanges::MIXED_SQL_NULL},
    SortBottomPathCase{ObIntType, "30", "10", "price", true, true, true, SortBottomWrapper::BARE,
        SortBottomRanges::MIXED_SQL_NULL, true, NULL_FIRST},
    SortBottomPathCase{ObIntType, "30", "10", "price", true, true, false, SortBottomWrapper::BARE,
        SortBottomRanges::MIXED_SQL_NULL, false, NULL_LAST},
    SortBottomPathCase{ObIntType, "30", "10", "price", true, true, true, SortBottomWrapper::BARE,
        SortBottomRanges::MIXED_SQL_NULL, false, NULL_FIRST},
    SortBottomPathCase{ObJsonType, "30", "10", "price", true, true, true, SortBottomWrapper::JSON_NULL,
        SortBottomRanges::SEPARATE_JSON_NULLS},
    SortBottomPathCase{ObJsonType, "30", "10", "price", true, true, true, SortBottomWrapper::JSON_NULL,
        SortBottomRanges::SEPARATE_JSON_NULLS, true, NULL_FIRST},
    SortBottomPathCase{ObJsonType, "30", "10", "price", true, true, true, SortBottomWrapper::JSON_NULL,
        SortBottomRanges::SEPARATE_JSON_NULLS, false, NULL_FIRST},
    SortBottomPathCase{ObJsonType, "30", "10", "price", true, true, false, SortBottomWrapper::JSON_NULL,
        SortBottomRanges::SEPARATE_JSON_NULLS, false, NULL_LAST},
    SortBottomPathCase{ObJsonType, "30", "10", "price", true, true, false, SortBottomWrapper::JSON_NULL,
        SortBottomRanges::CROSS_JSON_TYPES}));

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

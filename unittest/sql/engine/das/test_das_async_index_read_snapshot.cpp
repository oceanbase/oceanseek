/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_async_token_op.h"
#include "sql/das/search/ob_das_async_token_helper.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_scalar_index_ror_op.h"
#include "sql/das/search/ob_das_scalar_primary_ror_op.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "sql/engine/ob_exec_context.h"
#undef protected
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{

struct MockAsyncSnapshotFetcherCtx
{
  MockAsyncSnapshotFetcherCtx() : snapshots_(), fetch_idx_(0) {}
  ObSEArray<int64_t, 2> snapshots_;
  int64_t fetch_idx_;
};

int mock_async_snapshot_fetcher(
    void *arg,
    const share::ObLSID &ls_id,
    const ObIArray<uint64_t> &index_table_ids,
    const ObIArray<ObTabletID> &index_tablet_ids,
    const ObTabletID &delete_tablet_id,
    const share::SCN &sql_read_snapshot,
    ObAsyncIndexPartitionSegCtx &out_seg_ctx)
{
  int ret = OB_SUCCESS;
  MockAsyncSnapshotFetcherCtx *fetcher_ctx =
      static_cast<MockAsyncSnapshotFetcherCtx *>(arg);
  UNUSED(ls_id);
  UNUSED(delete_tablet_id);
  UNUSED(sql_read_snapshot);
  if (OB_ISNULL(fetcher_ctx)
      || OB_UNLIKELY(fetcher_ctx->fetch_idx_ >= fetcher_ctx->snapshots_.count()
                     || 1 != index_table_ids.count()
                     || index_table_ids.count() != index_tablet_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t snapshot = fetcher_ctx->snapshots_.at(fetcher_ctx->fetch_idx_);
    storage::ObAsyncIndexSegmentGroup *group = nullptr;
    ObNewVersionRange range;
    range.base_version_ = 0;
    range.snapshot_version_ = snapshot;
    if (OB_FAIL(out_seg_ctx.storage_group_ctx_.async_index_read_snapshot_.convert_for_tx(
            snapshot))) {
    } else if (OB_ISNULL(out_seg_ctx.storage_group_ctx_.index_tablet_handles_
                             .alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(group = OB_NEWx(
                   storage::ObAsyncIndexSegmentGroup,
                   &out_seg_ctx.storage_group_ctx_.allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(group->init(1))) {
    } else if (OB_FAIL(group->add_version_range(0, range))) {
    } else if (OB_FAIL(out_seg_ctx.storage_group_ctx_.groups_.push_back(group))) {
    } else {
      out_seg_ctx.storage_group_ctx_.is_strict_aligned_ = true;
      ++fetcher_ctx->fetch_idx_;
    }
  }
  return ret;
}

int init_sql_snapshot(
    const int64_t snapshot_value,
    share::SCN &sql_read_snapshot,
    transaction::ObTxReadSnapshot &sql_snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_read_snapshot.convert_for_tx(snapshot_value))) {
  } else {
    sql_snapshot.init_special_read(sql_read_snapshot);
  }
  return ret;
}

int prepare_mock_async_query_ctx(
    MockAsyncSnapshotFetcherCtx &fetcher_ctx,
    const share::SCN &sql_read_snapshot,
    ObDASAsyncIndexQueryCtx &async_query_ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> index_table_ids;
  ObSEArray<ObTabletID, 1> index_tablet_ids;
  const ObTabletID delete_tablet_id(3001);
  if (OB_FAIL(fetcher_ctx.snapshots_.push_back(120))) {
  } else if (OB_FAIL(fetcher_ctx.snapshots_.push_back(80))) {
  } else if (OB_FAIL(index_table_ids.push_back(4001))) {
  } else if (OB_FAIL(index_tablet_ids.push_back(ObTabletID(2001)))) {
  } else {
    async_query_ctx.set_mock_fetcher(mock_async_snapshot_fetcher, &fetcher_ctx);
    if (OB_FAIL(async_query_ctx.add_partition(
            share::ObLSID(1001), ObTabletID(10001), index_table_ids,
            index_tablet_ids, delete_tablet_id, sql_read_snapshot))) {
    } else if (OB_FAIL(async_query_ctx.add_partition(
                   share::ObLSID(1001), ObTabletID(10002), index_table_ids,
                   index_tablet_ids, delete_tablet_id, sql_read_snapshot))) {
    } else if (OB_FAIL(async_query_ctx.finalize())) {
    }
  }
  return ret;
}

int init_scalar_scan_rowid_metadata(
    ObExpr &rowid_expr,
    ExprFixedArray &rowid_exprs,
    ObDASScalarScanCtDef &scalar_ctdef,
    ObDASSearchCtx &search_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scalar_ctdef.rowkey_exprs_.init(1))) {
  } else if (OB_FAIL(scalar_ctdef.rowkey_exprs_.push_back(&rowid_expr))) {
  } else if (OB_FAIL(rowid_exprs.init(1))) {
  } else if (OB_FAIL(rowid_exprs.push_back(&rowid_expr))) {
  } else {
    search_ctx.rowid_exprs_ = &rowid_exprs;
    search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs;
  }
  return ret;
}

class MockScalarRowIterator : public ObNewRowIterator
{
public:
  int get_next_row(ObNewRow *&row) override
  {
    row = nullptr;
    return OB_ITER_END;
  }
  void reset() override {}
};

class MockScalarAccessService : public storage::ObAccessService
{
public:
  MockScalarAccessService()
    : expected_params_(),
      expected_snapshots_(),
      expected_switch_params_(),
      scan_call_count_(0),
      reuse_call_count_(0),
      rescan_call_count_(0),
      revert_call_count_(0),
      row_iter_()
  {}

  int expect_scan_param(
      const storage::ObTableScanParam &scan_param,
      const int64_t snapshot_value,
      const bool switch_param = true)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(expected_params_.push_back(&scan_param))) {
    } else if (OB_FAIL(expected_snapshots_.push_back(snapshot_value))) {
    } else if (OB_FAIL(expected_switch_params_.push_back(switch_param))) {
    }
    return ret;
  }

  int table_scan(common::ObVTableScanParam &vparam, ObNewRowIterator *&result) override
  {
    int ret = OB_SUCCESS;
    storage::ObTableScanParam &scan_param =
        static_cast<storage::ObTableScanParam &>(vparam);
    ++scan_call_count_;
    if (1 != scan_call_count_ || expected_params_.empty()
        || expected_params_.at(0) != &scan_param
        || expected_snapshots_.at(0) != scan_param.snapshot_.version().get_val_for_tx()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      result = &row_iter_;
    }
    return ret;
  }

  int revert_scan_iter(ObNewRowIterator *iter) override
  {
    int ret = OB_SUCCESS;
    ++revert_call_count_;
    if (&row_iter_ != iter) {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  int reuse_scan_iter(
      const bool switch_param,
      common::ObNewRowIterator *iter) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(reuse_call_count_ >= expected_params_.count()
                    || (scan_call_count_ > 0 && &row_iter_ != iter))) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const storage::ObTableScanParam *scan_param =
          expected_params_.at(reuse_call_count_);
      if (OB_ISNULL(scan_param)
          || expected_switch_params_.at(reuse_call_count_) != switch_param
          || expected_snapshots_.at(reuse_call_count_)
                 != scan_param->snapshot_.version().get_val_for_tx()) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ++reuse_call_count_;
      }
    }
    return ret;
  }

  int table_rescan(
      common::ObVTableScanParam &vparam,
      common::ObNewRowIterator *result) override
  {
    int ret = OB_SUCCESS;
    storage::ObTableScanParam &scan_param =
        static_cast<storage::ObTableScanParam &>(vparam);
    if (OB_UNLIKELY(rescan_call_count_ >= expected_params_.count()
                    || (scan_call_count_ > 0 && &row_iter_ != result)
                    || expected_params_.at(rescan_call_count_) != &scan_param
                    || expected_snapshots_.at(rescan_call_count_)
                           != scan_param.snapshot_.version().get_val_for_tx())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ++rescan_call_count_;
    }
    return ret;
  }

  int64_t get_scan_call_count() const { return scan_call_count_; }
  int64_t get_reuse_call_count() const { return reuse_call_count_; }
  int64_t get_rescan_call_count() const { return rescan_call_count_; }
  int64_t get_revert_call_count() const { return revert_call_count_; }

private:
  ObSEArray<const storage::ObTableScanParam *, 2> expected_params_;
  ObSEArray<int64_t, 2> expected_snapshots_;
  ObSEArray<bool, 2> expected_switch_params_;
  int64_t scan_call_count_;
  int64_t reuse_call_count_;
  int64_t rescan_call_count_;
  int64_t revert_call_count_;
  MockScalarRowIterator row_iter_;
};

class ObDASAsyncIndexReadSnapshotTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObDASAsyncIndexReadSnapshotTest, primary_scalar_shallow_preserves_score_bound)
{
  ObArenaAllocator alloc("ScalarScore");
  ObDASScanOp root_scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, root_scan_op);
  ObDASScalarScanCtDef ctdef(alloc);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ExprFixedArray rowid_exprs(alloc);
  ASSERT_EQ(OB_SUCCESS, init_scalar_scan_rowid_metadata(
      rowid_expr, rowid_exprs, ctdef, search_ctx));
  UInt64RowIDStore store(search_ctx);
  ASSERT_EQ(OB_SUCCESS, store.init(2));
  store.data_[0] = 1;
  store.data_[1] = 3;
  store.count_ = 2;
  rtdef.set_boost_weight(2.5);
  MockScalarRowIterator row_iter;

  for (const bool scoring : {false, true}) {
    ObDASScalarPrimaryROROp op(search_ctx);
    ObDASScalarPrimaryROROpParam param(&ctdef, &rtdef);
    param.set_is_scoring(scoring);
    ASSERT_EQ(OB_SUCCESS, op.init(param));
    op.rowid_store_ = &store;
    op.result_ = &row_iter;
    op.rowid_store_iter_.init(&store);
    op.rowid_store_iter_.reuse();
    const double expected_score = scoring ? 2.5 : 0.0;
    const MaxScoreTuple *bound = nullptr;
    ObDASRowID target;
    target.set_uint64(1);
    ASSERT_EQ(OB_SUCCESS, op.do_advance_shallow(target, true, bound));
    ASSERT_NE(nullptr, bound);
    EXPECT_DOUBLE_EQ(expected_score, bound->get_max_score());
    EXPECT_EQ(1, bound->get_min_id().get_uint64());
    EXPECT_EQ(1, bound->get_max_id().get_uint64());
    ASSERT_EQ(OB_SUCCESS, op.do_advance_shallow(target, false, bound));
    EXPECT_DOUBLE_EQ(expected_score, bound->get_max_score());
    EXPECT_EQ(3, bound->get_min_id().get_uint64());
    EXPECT_EQ(3, bound->get_max_id().get_uint64());
    double max_score = -1.0;
    ASSERT_EQ(OB_SUCCESS, op.do_calc_max_score(max_score));
    EXPECT_DOUBLE_EQ(expected_score, max_score);
  }
}

TEST_F(ObDASAsyncIndexReadSnapshotTest, index_scalar_batch_preserves_scores_and_successor)
{
  class EmptyBatchIterator : public MockScalarRowIterator
  {
  public:
    int get_next_rows(int64_t &count, int64_t capacity) override
    {
      UNUSED(capacity);
      count = 0;
      return OB_ITER_END;
    }
  } empty_iter;
  ObArenaAllocator alloc("ScalarScore");
  ObDASScanOp root_scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, root_scan_op);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  eval_ctx.max_batch_size_ = 8;
  search_ctx.eval_ctx_ = &eval_ctx;
  ObDASScalarScanCtDef ctdef(alloc);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ExprFixedArray rowid_exprs(alloc);
  ASSERT_EQ(OB_SUCCESS, init_scalar_scan_rowid_metadata(
      rowid_expr, rowid_exprs, ctdef, search_ctx));
  UInt64RowIDStore store(search_ctx);
  ASSERT_EQ(OB_SUCCESS, store.init(4));
  rtdef.set_boost_weight(2.5);

  for (const bool scoring : {false, true}) {
    store.data_[0] = 1;
    store.data_[1] = 2;
    store.data_[2] = 4;
    store.data_[3] = 8;
    store.count_ = 4;
    ObDASScalarIndexROROp op(search_ctx);
    ObDASScalarIndexROROpParam param(&ctdef, &rtdef);
    param.set_is_scoring(scoring);
    ASSERT_EQ(OB_SUCCESS, op.init(param));
    op.rowid_store_ = &store;
    op.rowid_store_iter_.init(&store);
    op.rowid_store_iter_.reuse();
    op.result_ = &empty_iter;
    const double expected_score = scoring ? 2.5 : 0.0;
    uint64_t ids[4] = {};
    double scores[4] = {};
    int64_t count = 0;
    ObDASRowID boundary;
    ObDASRowID next;
    double next_score = -1.0;

    // A full batch consumes only the next row, leaving the successor unspecified.
    boundary.set_uint64(4);
    ASSERT_EQ(OB_SUCCESS, op.drain_until(boundary, 1, ids, scores, count, next, next_score));
    ASSERT_EQ(1, count);
    EXPECT_EQ(2, ids[0]);
    EXPECT_DOUBLE_EQ(expected_score, scores[0]);
    // Continue the same boundary, returning the first row beyond it with its score.
    ASSERT_EQ(OB_SUCCESS, op.drain_until(boundary, 4, ids, scores, count, next, next_score));
    ASSERT_EQ(1, count);
    EXPECT_EQ(4, ids[0]);
    EXPECT_DOUBLE_EQ(expected_score, scores[0]);
    EXPECT_EQ(8, next.get_uint64());
    EXPECT_DOUBLE_EQ(expected_score, next_score);
    boundary.set_max();
    ASSERT_EQ(OB_SUCCESS, op.drain_until(boundary, 4, ids, scores, count, next, next_score));
    EXPECT_EQ(0, count);
    EXPECT_TRUE(next.is_max());
    EXPECT_DOUBLE_EQ(0.0, next_score);

    store.count_ = 4;
    op.rowid_store_iter_.reuse();
    // An empty bounded batch still returns a scored successor without a score buffer.
    boundary.set_uint64(1);
    ASSERT_EQ(OB_SUCCESS, op.drain_until(boundary, 4, ids, nullptr, count, next, next_score));
    EXPECT_EQ(0, count);
    EXPECT_EQ(2, next.get_uint64());
    EXPECT_DOUBLE_EQ(expected_score, next_score);
    // MAX boundary must preserve every remaining score through EOF.
    boundary.set_max();
    ASSERT_EQ(OB_SUCCESS, op.drain_until(boundary, 4, ids, scores, count, next, next_score));
    ASSERT_EQ(2, count);
    EXPECT_EQ(4, ids[0]);
    EXPECT_EQ(8, ids[1]);
    EXPECT_DOUBLE_EQ(expected_score, scores[0]);
    EXPECT_DOUBLE_EQ(expected_score, scores[1]);
    EXPECT_TRUE(next.is_max());
    EXPECT_DOUBLE_EQ(0.0, next_score);
  }
}

class ObDASScalarPrimaryOpenTest : public ObDASAsyncIndexReadSnapshotTest,
                                 public ::testing::WithParamInterface<bool>
{
protected:
  void SetUp() override
  {
    saved_tenant_id_ = MTL_ID();
    saved_access_service_ = MTL(storage::ObAccessService *);
    ObDASAsyncIndexReadSnapshotTest::SetUp();
    share::ObTenantEnv::get_tenant_local()->set<storage::ObAccessService *>(&access_service_);
  }

  void TearDown() override
  {
    share::ObTenantEnv::get_tenant_local()->set<storage::ObAccessService *>(saved_access_service_);
    share::ObTenantEnv::get_tenant_local()->id_ = saved_tenant_id_;
  }

  MockScalarAccessService access_service_;
  storage::ObAccessService *saved_access_service_ = nullptr;
  uint64_t saved_tenant_id_ = OB_INVALID_TENANT_ID;
};

TEST_P(ObDASScalarPrimaryOpenTest, open_and_rescan_only_active_iterator)
{
  const bool probe_mode = GetParam();
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp root_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(scan_alloc, root_scan_op);
  ObExecContext exec_ctx(scan_alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASScalarScanCtDef scalar_ctdef(scan_alloc);
  ObDASScalarScanRtDef scalar_rtdef;
  ObExpr rowid_expr;
  ExprFixedArray rowid_exprs(scan_alloc);
  share::SCN sql_read_snapshot;
  transaction::ObTxReadSnapshot sql_snapshot;
  ObObj start_key;
  ObObj end_key;
  ObNewRange range;
  ObDASScalarPrimaryROROp primary_ror_op(search_ctx);
  ObDASScalarPrimaryROROpParam op_param(&scalar_ctdef, &scalar_rtdef);
  storage::ObTableScanParam &active_param = probe_mode
      ? primary_ror_op.get_param_ : primary_ror_op.scan_param_;
  storage::ObTableScanParam &unused_param = probe_mode
      ? primary_ror_op.scan_param_ : primary_ror_op.get_param_;

  ASSERT_EQ(OB_SUCCESS, init_sql_snapshot(300, sql_read_snapshot, sql_snapshot));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.set_related_task_info(
      &scalar_ctdef, &scalar_rtdef, ObTabletID(10002)));
  ASSERT_EQ(OB_SUCCESS, init_scalar_scan_rowid_metadata(
      rowid_expr, rowid_exprs, scalar_ctdef, search_ctx));
  eval_ctx.max_batch_size_ = 8;
  search_ctx.eval_ctx_ = &eval_ctx;
  search_ctx.snapshot_ = &sql_snapshot;
  search_ctx.set_ls_id(share::ObLSID(1001));
  scalar_rtdef.eval_ctx_ = &eval_ctx;
  scalar_rtdef.stmt_allocator_.set_alloc(&scan_alloc);
  scalar_rtdef.scan_allocator_.set_alloc(&scan_alloc);
  ASSERT_EQ(OB_SUCCESS, scalar_rtdef.init_pd_op(exec_ctx, scalar_ctdef));
  start_key.set_min_value();
  end_key.set_max_value();
  range.start_key_.assign(&start_key, 1);
  range.end_key_.assign(&end_key, 1);
  ASSERT_EQ(OB_SUCCESS, scalar_rtdef.key_ranges_.push_back(range));
  op_param.set_is_probe_mode(probe_mode);
  ASSERT_EQ(OB_SUCCESS, primary_ror_op.init(op_param));
  ASSERT_EQ(OB_SUCCESS, access_service_.expect_scan_param(active_param, 300, false));

  ASSERT_EQ(OB_SUCCESS, primary_ror_op.do_open());
  EXPECT_EQ(1, access_service_.get_scan_call_count());
  EXPECT_EQ(probe_mode, active_param.is_get_);
  ASSERT_EQ(1, primary_ror_op.rowid_output_exprs_.get_exprs().count());
  EXPECT_EQ(&rowid_expr, primary_ror_op.rowid_output_exprs_.get_exprs().at(0));
  EXPECT_EQ(probe_mode, nullptr == primary_ror_op.result_);
  EXPECT_EQ(probe_mode, nullptr == primary_ror_op.rowid_store_);
  EXPECT_EQ(!probe_mode, nullptr == primary_ror_op.get_result_);

  // Rescan must touch only the iterator opened for the selected mode.
  ASSERT_EQ(OB_SUCCESS, primary_ror_op.do_rescan());
  EXPECT_EQ(1, access_service_.get_reuse_call_count());
  EXPECT_EQ(1, access_service_.get_rescan_call_count());
  EXPECT_FALSE(unused_param.tablet_id_.is_valid());
  EXPECT_TRUE(unused_param.key_ranges_.empty());

  ASSERT_EQ(OB_SUCCESS, primary_ror_op.do_close());
  EXPECT_EQ(1, access_service_.get_revert_call_count());
}

INSTANTIATE_TEST_CASE_P(ScanAndProbe, ObDASScalarPrimaryOpenTest, ::testing::Bool());

TEST_F(ObDASAsyncIndexReadSnapshotTest,
       search_context_rebinds_current_partition_snapshot)
{
  MockAsyncSnapshotFetcherCtx fetcher_ctx;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(scan_alloc, scan_op);
  ObDASAsyncIndexQueryCtx async_query_ctx;
  share::SCN sql_read_snapshot;
  transaction::ObTxReadSnapshot sql_snapshot;
  storage::ObTableScanParam scan_param;
  const ObAsyncIndexPartitionSegCtx *partition = nullptr;

  ASSERT_EQ(OB_SUCCESS,
            init_sql_snapshot(300, sql_read_snapshot, sql_snapshot));
  ASSERT_EQ(OB_SUCCESS, scan_param.snapshot_.assign(sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            prepare_mock_async_query_ctx(
                fetcher_ctx, sql_read_snapshot, async_query_ctx));
  search_ctx.set_async_query_ctx(&async_query_ctx);

  ASSERT_EQ(OB_SUCCESS,
            async_query_ctx.find_partition(ObTabletID(10001), partition));
  search_ctx.set_current_async_partition(partition);
  ASSERT_EQ(OB_SUCCESS,
            search_ctx.apply_async_index_read_snapshot(scan_param));
  EXPECT_EQ(120, scan_param.snapshot_.version().get_val_for_tx());

  ASSERT_EQ(OB_SUCCESS,
            async_query_ctx.find_partition(ObTabletID(10002), partition));
  search_ctx.set_current_async_partition(partition);
  ASSERT_EQ(OB_SUCCESS,
            search_ctx.apply_async_index_read_snapshot(scan_param));
  EXPECT_EQ(80, scan_param.snapshot_.version().get_val_for_tx());

  ASSERT_EQ(OB_SUCCESS,
            search_ctx.apply_async_index_read_snapshot(scan_param));
  EXPECT_EQ(80, scan_param.snapshot_.version().get_val_for_tx());
}

TEST_F(ObDASAsyncIndexReadSnapshotTest,
       search_context_rejects_invalid_async_state)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(scan_alloc, scan_op);
  ObDASAsyncIndexQueryCtx async_query_ctx;
  ObAsyncIndexPartitionSegCtx invalid_partition;
  share::SCN sql_read_snapshot;
  transaction::ObTxReadSnapshot sql_snapshot;
  storage::ObTableScanParam scan_param;

  ASSERT_EQ(OB_SUCCESS,
            init_sql_snapshot(300, sql_read_snapshot, sql_snapshot));
  ASSERT_EQ(OB_SUCCESS, scan_param.snapshot_.assign(sql_snapshot));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            search_ctx.apply_async_index_read_snapshot(scan_param));
  EXPECT_EQ(300, scan_param.snapshot_.version().get_val_for_tx());

  search_ctx.set_async_query_ctx(&async_query_ctx);
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            search_ctx.apply_async_index_read_snapshot(scan_param));
  EXPECT_EQ(300, scan_param.snapshot_.version().get_val_for_tx());

  search_ctx.set_current_async_partition(&invalid_partition);
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            search_ctx.apply_async_index_read_snapshot(scan_param));
  EXPECT_EQ(300, scan_param.snapshot_.version().get_val_for_tx());
}

TEST_F(ObDASAsyncIndexReadSnapshotTest,
       init_scan_param_preserves_sql_snapshot_when_async_snapshot_not_used)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp scan_op(scan_alloc);
  share::SCN sql_read_snapshot;
  transaction::ObTxReadSnapshot sql_snapshot;
  ASSERT_EQ(OB_SUCCESS,
            init_sql_snapshot(300, sql_read_snapshot, sql_snapshot));
  scan_op.set_snapshot(&sql_snapshot);

  ObDASSearchCtx search_ctx(scan_alloc, scan_op);
  ObDASAsyncIndexQueryCtx async_query_ctx;
  ObExecContext exec_ctx(scan_alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASScalarScanCtDef scalar_ctdef(scan_alloc);
  ObDASScalarScanRtDef scalar_rtdef;
  ObPushdownOperator pd_op(eval_ctx, scalar_ctdef.pd_expr_spec_);
  storage::ObTableScanParam sync_scan_param;
  storage::ObTableScanParam async_scan_param;
  ObTabletID tablet_id(10001);
  scalar_rtdef.p_pd_expr_op_ = &pd_op;

  ASSERT_EQ(OB_SUCCESS,
            search_ctx.init_scan_param(
                tablet_id, &scalar_ctdef, &scalar_rtdef, sync_scan_param,
                true /* use_async_read_snapshot */));
  EXPECT_EQ(300, sync_scan_param.snapshot_.version().get_val_for_tx());

  search_ctx.set_async_query_ctx(&async_query_ctx);
  ASSERT_EQ(OB_SUCCESS,
            search_ctx.init_scan_param(
                tablet_id, &scalar_ctdef, &scalar_rtdef, async_scan_param,
                false /* use_async_read_snapshot */));
  EXPECT_EQ(300, async_scan_param.snapshot_.version().get_val_for_tx());
}

TEST_F(ObDASAsyncIndexReadSnapshotTest,
       fts_posting_params_keep_sql_snapshot_across_partitions)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp scan_op(scan_alloc);
  share::SCN sql_read_snapshot;
  transaction::ObTxReadSnapshot sql_snapshot;
  ASSERT_EQ(OB_SUCCESS, init_sql_snapshot(300, sql_read_snapshot, sql_snapshot));
  scan_op.set_snapshot(&sql_snapshot);
  ObDASSearchCtx search_ctx(scan_alloc, scan_op);
  MockAsyncSnapshotFetcherCtx fetcher_ctx;
  ObDASAsyncIndexQueryCtx async_query_ctx;
  ASSERT_EQ(OB_SUCCESS, prepare_mock_async_query_ctx(
      fetcher_ctx, sql_read_snapshot, async_query_ctx));
  search_ctx.set_async_query_ctx(&async_query_ctx);
  search_ctx.set_ls_id(share::ObLSID(1001));

  ObExecContext exec_ctx(scan_alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASScalarScanCtDef inv_ctdef(scan_alloc);
  ObDASScalarScanRtDef inv_rtdef;
  ObPushdownOperator pd_op(eval_ctx, inv_ctdef.pd_expr_spec_);
  inv_rtdef.ctdef_ = &inv_ctdef;
  inv_rtdef.p_pd_expr_op_ = &pd_op;
  ObDASIRScanCtDef ir_ctdef(scan_alloc);
  ObDASIRScanRtDef ir_rtdef;
  ObDASBaseCtDef *ctdef_children[] = {&inv_ctdef};
  ObDASBaseRtDef *rtdef_children[] = {&inv_rtdef};
  ir_ctdef.children_ = ctdef_children;
  ir_ctdef.children_cnt_ = 1;
  ir_ctdef.use_scalar_scan_def_ = 1;
  ir_ctdef.is_async_index_ = 1;
  ir_rtdef.ctdef_ = &ir_ctdef;
  ir_rtdef.children_ = rtdef_children;
  ir_rtdef.children_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, scan_op.reserve_related_buffer(1));
  ASSERT_EQ(OB_SUCCESS, scan_op.set_related_task_info(
      &inv_ctdef, &inv_rtdef, ObTabletID(2001)));

  ObDASAsyncTokenOp token_op(search_ctx);
  ObDASAsyncTokenHelper token_helper(search_ctx);
  token_op.ir_ctdef_ = &ir_ctdef;
  token_op.ir_rtdef_ = &ir_rtdef;
  token_helper.ir_ctdef_ = &ir_ctdef;
  token_helper.ir_rtdef_ = &ir_rtdef;
  for (int64_t i = 0; i < 2; ++i) {
    const ObAsyncIndexPartitionSegCtx *partition = nullptr;
    ASSERT_EQ(OB_SUCCESS, async_query_ctx.find_partition(ObTabletID(10001 + i), partition));
    search_ctx.set_current_async_partition(partition);
    // Aligned watermarks (120 / 80) precede the refresh commit at 250.
    // Both posting entry points must retain SQL snapshot 300 for visibility.
    ASSERT_EQ(OB_SUCCESS, token_op.init_inv_idx_scan_param());
    ASSERT_EQ(OB_SUCCESS, token_helper.init_inv_idx_scan_param());
    EXPECT_EQ(300, token_op.inv_idx_scan_param_.snapshot_.version().get_val_for_tx());
    EXPECT_EQ(300, token_helper.inv_idx_scan_param_.snapshot_.version().get_val_for_tx());
    EXPECT_EQ(0 == i ? 120 : 80, partition->get_read_snapshot().get_val_for_tx());
  }
}

TEST_F(ObDASAsyncIndexReadSnapshotTest,
       scalar_rescans_preserve_sync_snapshot_without_async_query)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp root_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(scan_alloc, root_scan_op);
  ObDASScalarScanCtDef scalar_ctdef(scan_alloc);
  ObDASScalarScanRtDef scalar_rtdef;
  ObExpr rowid_expr;
  ExprFixedArray rowid_exprs(scan_alloc);
  share::SCN sql_read_snapshot;
  transaction::ObTxReadSnapshot sql_snapshot;

  ASSERT_EQ(OB_SUCCESS,
            init_sql_snapshot(300, sql_read_snapshot, sql_snapshot));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.set_related_task_info(
      &scalar_ctdef, &scalar_rtdef, ObTabletID(10002)));
  search_ctx.set_ls_id(share::ObLSID(1001));
  ASSERT_EQ(OB_SUCCESS,
            init_scalar_scan_rowid_metadata(
                rowid_expr, rowid_exprs, scalar_ctdef, search_ctx));

  MockScalarAccessService scalar_access_service;
  ObDASScalarScanOp scalar_scan_op(search_ctx);
  ObDASScalarScanOpParam scalar_scan_op_param(&scalar_ctdef, &scalar_rtdef);
  ASSERT_EQ(OB_SUCCESS, scalar_scan_op.init(scalar_scan_op_param));
  scalar_scan_op.tsc_service_ = &scalar_access_service;
  scalar_scan_op.scan_param_.tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS,
            scalar_scan_op.scan_param_.snapshot_.assign(sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            scalar_access_service.expect_scan_param(
                scalar_scan_op.scan_param_, 300));
  ASSERT_EQ(OB_SUCCESS, scalar_scan_op.do_rescan());
  EXPECT_EQ(300,
            scalar_scan_op.scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(1, scalar_access_service.get_reuse_call_count());
  EXPECT_EQ(1, scalar_access_service.get_rescan_call_count());

  MockScalarAccessService primary_ror_access_service;
  ObDASScalarPrimaryROROp primary_ror_op(search_ctx);
  ObDASScalarPrimaryROROpParam primary_ror_op_param(
      &scalar_ctdef, &scalar_rtdef);
  primary_ror_op_param.set_is_probe_mode(true);
  ASSERT_EQ(OB_SUCCESS, primary_ror_op.init(primary_ror_op_param));
  primary_ror_op.tsc_service_ = &primary_ror_access_service;
  primary_ror_op.scan_param_.tablet_id_ = ObTabletID(10001);
  primary_ror_op.get_param_.tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS,
            primary_ror_op.scan_param_.snapshot_.assign(sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            primary_ror_op.get_param_.snapshot_.assign(sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            primary_ror_access_service.expect_scan_param(
                primary_ror_op.get_param_, 300));
  ASSERT_EQ(OB_SUCCESS, primary_ror_op.do_rescan());
  EXPECT_EQ(300,
            primary_ror_op.scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(300,
            primary_ror_op.get_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(ObTabletID(10001), primary_ror_op.scan_param_.tablet_id_);
  EXPECT_EQ(ObTabletID(10002), primary_ror_op.get_param_.tablet_id_);
  EXPECT_EQ(1, primary_ror_access_service.get_reuse_call_count());
  EXPECT_EQ(1, primary_ror_access_service.get_rescan_call_count());
}

TEST_F(ObDASAsyncIndexReadSnapshotTest,
       scalar_rescans_rebind_snapshot_before_storage_calls)
{
  MockAsyncSnapshotFetcherCtx fetcher_ctx;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp root_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(scan_alloc, root_scan_op);
  ObDASAsyncIndexQueryCtx async_query_ctx;
  ObDASScalarScanCtDef scalar_ctdef(scan_alloc);
  ObDASScalarScanRtDef scalar_rtdef;
  ObExpr rowid_expr;
  ExprFixedArray rowid_exprs(scan_alloc);
  share::SCN sql_read_snapshot;
  transaction::ObTxReadSnapshot sql_snapshot;
  const ObAsyncIndexPartitionSegCtx *partition = nullptr;

  ASSERT_EQ(OB_SUCCESS,
            init_sql_snapshot(300, sql_read_snapshot, sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            prepare_mock_async_query_ctx(
                fetcher_ctx, sql_read_snapshot, async_query_ctx));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.set_related_task_info(
      &scalar_ctdef, &scalar_rtdef, ObTabletID(10002)));
  ASSERT_EQ(OB_SUCCESS,
            async_query_ctx.find_partition(ObTabletID(10002), partition));
  search_ctx.set_ls_id(share::ObLSID(1001));
  search_ctx.set_async_query_ctx(&async_query_ctx);
  search_ctx.set_current_async_partition(partition);
  ASSERT_EQ(OB_SUCCESS,
            init_scalar_scan_rowid_metadata(
                rowid_expr, rowid_exprs, scalar_ctdef, search_ctx));

  MockScalarAccessService scalar_access_service;
  ObDASScalarScanOp scalar_scan_op(search_ctx);
  ObDASScalarScanOpParam scalar_scan_op_param(&scalar_ctdef, &scalar_rtdef);
  ASSERT_EQ(OB_SUCCESS, scalar_scan_op.init(scalar_scan_op_param));
  scalar_scan_op.tsc_service_ = &scalar_access_service;
  scalar_scan_op.scan_param_.tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS,
            scalar_scan_op.scan_param_.snapshot_.assign(sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            scalar_access_service.expect_scan_param(
                scalar_scan_op.scan_param_, 80));
  ASSERT_EQ(OB_SUCCESS, scalar_scan_op.do_rescan());
  EXPECT_EQ(80,
            scalar_scan_op.scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(1, scalar_access_service.get_reuse_call_count());
  EXPECT_EQ(1, scalar_access_service.get_rescan_call_count());

  MockScalarAccessService index_ror_access_service;
  ObDASScalarIndexROROp index_ror_op(search_ctx);
  ObDASScalarIndexROROpParam index_ror_op_param(
      &scalar_ctdef, &scalar_rtdef);
  ASSERT_EQ(OB_SUCCESS, index_ror_op.init(index_ror_op_param));
  index_ror_op.tsc_service_ = &index_ror_access_service;
  index_ror_op.scan_param_.tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS,
            index_ror_op.scan_param_.snapshot_.assign(sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            index_ror_access_service.expect_scan_param(
                index_ror_op.scan_param_, 80));
  ASSERT_EQ(OB_SUCCESS, index_ror_op.do_rescan());
  EXPECT_EQ(80,
            index_ror_op.scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(1, index_ror_access_service.get_reuse_call_count());
  EXPECT_EQ(1, index_ror_access_service.get_rescan_call_count());

  MockScalarAccessService primary_ror_access_service;
  ObDASScalarPrimaryROROp primary_ror_op(search_ctx);
  ObDASScalarPrimaryROROpParam primary_ror_op_param(
      &scalar_ctdef, &scalar_rtdef);
  primary_ror_op_param.set_is_probe_mode(true);
  ASSERT_EQ(OB_SUCCESS, primary_ror_op.init(primary_ror_op_param));
  primary_ror_op.tsc_service_ = &primary_ror_access_service;
  primary_ror_op.scan_param_.tablet_id_ = ObTabletID(10001);
  primary_ror_op.get_param_.tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS,
            primary_ror_op.scan_param_.snapshot_.assign(sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            primary_ror_op.get_param_.snapshot_.assign(sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            primary_ror_access_service.expect_scan_param(
                primary_ror_op.get_param_, 80));
  ASSERT_EQ(OB_SUCCESS, primary_ror_op.do_rescan());
  // Probe mode must not rebind or rescan the unused ordinary scan.
  EXPECT_EQ(300,
            primary_ror_op.scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(80,
            primary_ror_op.get_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(ObTabletID(10001), primary_ror_op.scan_param_.tablet_id_);
  EXPECT_EQ(ObTabletID(10002), primary_ror_op.get_param_.tablet_id_);
  EXPECT_EQ(1, primary_ror_access_service.get_reuse_call_count());
  EXPECT_EQ(1, primary_ror_access_service.get_rescan_call_count());
}

TEST_F(ObDASAsyncIndexReadSnapshotTest,
       scalar_rescan_snapshot_errors_precede_storage_calls)
{
  MockAsyncSnapshotFetcherCtx fetcher_ctx;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp root_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(scan_alloc, root_scan_op);
  ObDASAsyncIndexQueryCtx async_query_ctx;
  ObDASScalarScanCtDef scalar_ctdef(scan_alloc);
  ObDASScalarScanRtDef scalar_rtdef;
  ObExpr rowid_expr;
  ExprFixedArray rowid_exprs(scan_alloc);
  ObAsyncIndexPartitionSegCtx invalid_partition;
  share::SCN sql_read_snapshot;
  transaction::ObTxReadSnapshot sql_snapshot;

  ASSERT_EQ(OB_SUCCESS,
            init_sql_snapshot(300, sql_read_snapshot, sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            prepare_mock_async_query_ctx(
                fetcher_ctx, sql_read_snapshot, async_query_ctx));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.set_related_task_info(
      &scalar_ctdef, &scalar_rtdef, ObTabletID(10002)));
  search_ctx.set_ls_id(share::ObLSID(1001));
  search_ctx.set_async_query_ctx(&async_query_ctx);
  search_ctx.set_current_async_partition(&invalid_partition);
  ASSERT_EQ(OB_SUCCESS,
            init_scalar_scan_rowid_metadata(
                rowid_expr, rowid_exprs, scalar_ctdef, search_ctx));

  MockScalarAccessService scalar_access_service;
  ObDASScalarScanOp scalar_scan_op(search_ctx);
  ObDASScalarScanOpParam scalar_scan_op_param(&scalar_ctdef, &scalar_rtdef);
  ASSERT_EQ(OB_SUCCESS, scalar_scan_op.init(scalar_scan_op_param));
  scalar_scan_op.tsc_service_ = &scalar_access_service;
  scalar_scan_op.scan_param_.tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS,
            scalar_scan_op.scan_param_.snapshot_.assign(sql_snapshot));
  EXPECT_EQ(OB_ERR_UNEXPECTED, scalar_scan_op.do_rescan());
  EXPECT_EQ(300,
            scalar_scan_op.scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(0, scalar_access_service.get_reuse_call_count());
  EXPECT_EQ(0, scalar_access_service.get_rescan_call_count());

  MockScalarAccessService index_ror_access_service;
  ObDASScalarIndexROROp index_ror_op(search_ctx);
  ObDASScalarIndexROROpParam index_ror_op_param(
      &scalar_ctdef, &scalar_rtdef);
  ASSERT_EQ(OB_SUCCESS, index_ror_op.init(index_ror_op_param));
  index_ror_op.tsc_service_ = &index_ror_access_service;
  index_ror_op.scan_param_.tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS,
            index_ror_op.scan_param_.snapshot_.assign(sql_snapshot));
  EXPECT_EQ(OB_ERR_UNEXPECTED, index_ror_op.do_rescan());
  EXPECT_EQ(300,
            index_ror_op.scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(0, index_ror_access_service.get_reuse_call_count());
  EXPECT_EQ(0, index_ror_access_service.get_rescan_call_count());

  MockScalarAccessService primary_ror_access_service;
  ObDASScalarPrimaryROROp primary_ror_op(search_ctx);
  ObDASScalarPrimaryROROpParam primary_ror_op_param(
      &scalar_ctdef, &scalar_rtdef);
  primary_ror_op_param.set_is_probe_mode(true);
  ASSERT_EQ(OB_SUCCESS, primary_ror_op.init(primary_ror_op_param));
  primary_ror_op.tsc_service_ = &primary_ror_access_service;
  primary_ror_op.scan_param_.tablet_id_ = ObTabletID(10001);
  primary_ror_op.get_param_.tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS,
            primary_ror_op.scan_param_.snapshot_.assign(sql_snapshot));
  ASSERT_EQ(OB_SUCCESS,
            primary_ror_op.get_param_.snapshot_.assign(sql_snapshot));
  EXPECT_EQ(OB_ERR_UNEXPECTED, primary_ror_op.do_rescan());
  EXPECT_EQ(300,
            primary_ror_op.scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(300,
            primary_ror_op.get_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(0, primary_ror_access_service.get_reuse_call_count());
  EXPECT_EQ(0, primary_ror_access_service.get_rescan_call_count());
}

TEST_F(ObDASAsyncIndexReadSnapshotTest, local_lookup_uses_current_partition_async_snapshot)
{
  MockAsyncSnapshotFetcherCtx fetcher_ctx;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(scan_alloc, scan_op);
  ObDASAsyncIndexQueryCtx async_query_ctx;
  ObSEArray<uint64_t, 1> index_table_ids;
  ObSEArray<ObTabletID, 1> index_tablet_ids;
  ObTabletID delete_tablet_id(3001);
  share::SCN sql_read_snapshot;
  transaction::ObTxReadSnapshot sql_snapshot;
  storage::ObTableScanParam lookup_param;
  ObDASLocalLookupIter lookup_iter;

  ASSERT_EQ(OB_SUCCESS, fetcher_ctx.snapshots_.push_back(120));
  ASSERT_EQ(OB_SUCCESS, fetcher_ctx.snapshots_.push_back(80));
  ASSERT_EQ(OB_SUCCESS, index_table_ids.push_back(4001));
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(ObTabletID(2001)));
  ASSERT_EQ(OB_SUCCESS, sql_read_snapshot.convert_for_tx(300));
  sql_snapshot.init_special_read(sql_read_snapshot);
  ASSERT_EQ(OB_SUCCESS, lookup_param.snapshot_.assign(sql_snapshot));

  async_query_ctx.set_mock_fetcher(mock_async_snapshot_fetcher, &fetcher_ctx);
  ASSERT_EQ(OB_SUCCESS, async_query_ctx.add_partition(
      share::ObLSID(1001), ObTabletID(10001), index_table_ids, index_tablet_ids,
      delete_tablet_id, sql_read_snapshot));
  ASSERT_EQ(OB_SUCCESS, async_query_ctx.add_partition(
      share::ObLSID(1001), ObTabletID(10002), index_table_ids, index_tablet_ids,
      delete_tablet_id, sql_read_snapshot));
  ASSERT_EQ(OB_SUCCESS, async_query_ctx.finalize());

  const ObAsyncIndexPartitionSegCtx *partition = nullptr;
  search_ctx.set_async_query_ctx(&async_query_ctx);
  ASSERT_EQ(OB_SUCCESS,
            async_query_ctx.find_partition(ObTabletID(10001), partition));
  search_ctx.set_current_async_partition(partition);
  lookup_iter.set_async_search_ctx(&search_ctx);
  ASSERT_EQ(OB_SUCCESS, lookup_iter.apply_async_index_read_snapshot_(lookup_param));
  EXPECT_EQ(120, lookup_param.snapshot_.version().get_val_for_tx());

  ASSERT_EQ(OB_SUCCESS,
            async_query_ctx.find_partition(ObTabletID(10002), partition));
  search_ctx.set_current_async_partition(partition);
  ASSERT_EQ(OB_SUCCESS, lookup_iter.apply_async_index_read_snapshot_(lookup_param));
  EXPECT_EQ(80, lookup_param.snapshot_.version().get_val_for_tx());
}

TEST_F(ObDASAsyncIndexReadSnapshotTest, async_delete_tablet_is_collected)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp scan_op(scan_alloc);
  ObDASBooleanQueryCtDef async_boolean_ctdef(scan_alloc);
  ObDASScalarScanCtDef match_ctdef(scan_alloc);
  ObDASScalarScanCtDef delete_ctdef(scan_alloc);
  ObDASBaseCtDef *children[] = {&match_ctdef, &delete_ctdef};
  ObTabletID delete_tablet_id;

  async_boolean_ctdef.children_ = children;
  async_boolean_ctdef.children_cnt_ = ARRAYSIZEOF(children);
  async_boolean_ctdef.set_must(0, 1);
  async_boolean_ctdef.set_must_not(1, 1);
  async_boolean_ctdef.set_is_async(true);
  ASSERT_EQ(OB_SUCCESS, scan_op.reserve_related_buffer(1));
  ASSERT_EQ(OB_SUCCESS, scan_op.set_related_task_info(
      &delete_ctdef, nullptr, ObTabletID(3001)));

  ASSERT_EQ(OB_SUCCESS, scan_op.collect_async_delete_tablet_id_from_ctdef_(
      &async_boolean_ctdef, delete_tablet_id));
  EXPECT_EQ(ObTabletID(3001), delete_tablet_id);
}

TEST_F(ObDASAsyncIndexReadSnapshotTest, multiple_async_delete_tablets_are_rejected)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp scan_op(scan_alloc);
  ObDASBooleanQueryCtDef first_boolean_ctdef(scan_alloc);
  ObDASBooleanQueryCtDef second_boolean_ctdef(scan_alloc);
  ObDASScalarScanCtDef first_delete_ctdef(scan_alloc);
  ObDASScalarScanCtDef second_delete_ctdef(scan_alloc);
  ObDASBaseCtDef *first_children[] = {&first_delete_ctdef};
  ObDASBaseCtDef *second_children[] = {&second_delete_ctdef};
  ObTabletID delete_tablet_id;

  first_boolean_ctdef.children_ = first_children;
  first_boolean_ctdef.children_cnt_ = ARRAYSIZEOF(first_children);
  first_boolean_ctdef.set_must_not(0, 1);
  first_boolean_ctdef.set_is_async(true);
  second_boolean_ctdef.children_ = second_children;
  second_boolean_ctdef.children_cnt_ = ARRAYSIZEOF(second_children);
  second_boolean_ctdef.set_must_not(0, 1);
  second_boolean_ctdef.set_is_async(true);
  ASSERT_EQ(OB_SUCCESS, scan_op.reserve_related_buffer(2));
  ASSERT_EQ(OB_SUCCESS, scan_op.set_related_task_info(
      &first_delete_ctdef, nullptr, ObTabletID(3001)));
  ASSERT_EQ(OB_SUCCESS, scan_op.set_related_task_info(
      &second_delete_ctdef, nullptr, ObTabletID(3002)));

  ASSERT_EQ(OB_SUCCESS, scan_op.collect_async_delete_tablet_id_from_ctdef_(
      &first_boolean_ctdef, delete_tablet_id));
  EXPECT_EQ(OB_ERR_UNEXPECTED, scan_op.collect_async_delete_tablet_id_from_ctdef_(
      &second_boolean_ctdef, delete_tablet_id));
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}

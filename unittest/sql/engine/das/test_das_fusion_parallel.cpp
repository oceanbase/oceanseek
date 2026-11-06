/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#include <algorithm>
#include <thread>
#include <chrono>
#include <atomic>
#include <limits>
#include <vector>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "storage/access/ob_async_inv_iterator_pool.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "sql/das/iter/ob_das_fusion_parallel.h"
#include "sql/das/iter/ob_das_fusion_iter.h"
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#include "sql/das/iter/ob_das_vec_index_ivf_scan_iter.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_async_segment_merge_op.h"
#include "sql/das/search/ob_das_async_segment_group_merge_op.h"
#include "sql/das/search/ob_das_dummy_op.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_das_scalar_index_ror_op.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "share/datum/ob_datum.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "lib/container/ob_se_array.h"
#include "common/object/ob_obj_type.h"
#undef private

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace
{

class TestEstimatedWorkSearchOp : public ObIDASSearchOp
{
public:
  TestEstimatedWorkSearchOp(ObDASSearchCtx &search_ctx,
                            const int64_t self_work_row_count)
    : ObIDASSearchOp(search_ctx),
      self_work_row_count_(self_work_row_count),
      is_probe_mode_(false),
      prepare_count_(0)
  {}

  void set_children(ObIDASSearchOp **children, const int64_t children_cnt)
  {
    children_ = children;
    children_cnt_ = children_cnt;
  }

  void set_work_role(const ObDASSearchOpType op_type,
                     const bool is_scoring)
  {
    op_type_ = op_type;
    is_scoring_ = is_scoring;
  }

  void set_probe_mode(const bool is_probe_mode)
  {
    is_probe_mode_ = is_probe_mode;
  }

  bool is_probe_mode() const override
  {
    return is_probe_mode_;
  }

  int64_t get_prepare_count() const
  {
    return prepare_count_;
  }

protected:
  int do_open() override { return OB_SUCCESS; }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override { return OB_SUCCESS; }
  int do_advance_to(const ObDASRowID &,
                    ObDASRowID &,
                    double &) override
  {
    return OB_SUCCESS;
  }
  int do_next_rowid(ObDASRowID &, double &) override
  {
    return OB_ITER_END;
  }
  int64_t get_self_estimated_work_row_count() const override
  {
    return self_work_row_count_;
  }
  int do_prepare_parallel_work_estimate() override
  {
    ++prepare_count_;
    return OB_SUCCESS;
  }

private:
  int64_t self_work_row_count_;
  bool is_probe_mode_;
  int64_t prepare_count_;
};

class TestEnumerableSearchOp : public ObIDASSearchOp
{
public:
  TestEnumerableSearchOp(
      ObDASSearchCtx &search_ctx,
      const std::vector<uint64_t> &rowids)
    : ObIDASSearchOp(search_ctx), rowids_(rowids), pos_(0)
  {
    op_type_ = DAS_SEARCH_OP_DUMMY;
    is_scoring_ = false;
  }

protected:
  int do_open() override
  {
    pos_ = 0;
    return OB_SUCCESS;
  }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override
  {
    pos_ = 0;
    return OB_SUCCESS;
  }
  int do_advance_to(
      const ObDASRowID &target,
      ObDASRowID &curr_id,
      double &score) override
  {
    if (target.is_max()) {
      pos_ = rowids_.size();
    } else {
      while (pos_ < rowids_.size()
             && target.is_normal()
             && rowids_[pos_] < target.get_uint64()) {
        ++pos_;
      }
    }
    return do_next_rowid(curr_id, score);
  }
  int do_next_rowid(ObDASRowID &next_id, double &score) override
  {
    int ret = OB_SUCCESS;
    if (pos_ >= rowids_.size()) {
      ret = OB_ITER_END;
    } else {
      next_id.set_uint64(rowids_[pos_++]);
      score = 0.0;
    }
    return ret;
  }

private:
  std::vector<uint64_t> rowids_;
  size_t pos_;
};

class TestFilterProviderFailureSourceParam : public ObIDASSearchOpParam
{
public:
  TestFilterProviderFailureSourceParam(
      const bool fail_open, const int64_t fail_after, const bool fail_rescan = false)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_DUMMY),
      fail_open_(fail_open), fail_after_(fail_after), fail_rescan_(fail_rescan)
  {}
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    UNUSED(children);
    return OB_SUCCESS;
  }
  bool fail_open_;
  int64_t fail_after_;
  bool fail_rescan_;
};

// Owned by SearchCtx like a production Source. The one-shot failure occurs
// after earlier next_rowid calls have already advanced the same drain batch.
class TestFilterProviderFailureSourceOp : public ObIDASSearchOp
{
public:
  explicit TestFilterProviderFailureSourceOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx), fail_open_(false), fail_after_(-1), fail_rescan_(false),
      failure_delivered_(false), resource_opened_(false), pos_(0),
      open_count_(0), rescan_count_(0), next_count_(0)
  {}
  int64_t get_consumed_count() const { return pos_; }
  int64_t get_open_count() const { return open_count_; }
  int64_t get_rescan_count() const { return rescan_count_; }
  int64_t get_next_count() const { return next_count_; }
  bool has_opened_resource() const { return resource_opened_; }

protected:
  int do_init(const ObIDASSearchOpParam &param) override
  {
    const TestFilterProviderFailureSourceParam &source_param =
        static_cast<const TestFilterProviderFailureSourceParam &>(param);
    fail_open_ = source_param.fail_open_;
    fail_after_ = source_param.fail_after_;
    fail_rescan_ = source_param.fail_rescan_;
    return OB_SUCCESS;
  }
  int do_open() override
  {
    int ret = OB_SUCCESS;
    ++open_count_;
    resource_opened_ = true;
    pos_ = 0;
    if (fail_open_ && !failure_delivered_) {
      failure_delivered_ = true;
      ret = OB_TIMEOUT;
    }
    return ret;
  }
  int do_close() override
  {
    resource_opened_ = false;
    return OB_SUCCESS;
  }
  int do_rescan() override
  {
    ++rescan_count_;
    pos_ = 0;
    return fail_rescan_ ? OB_TIMEOUT : OB_SUCCESS;
  }
  int do_advance_to(const ObDASRowID &, ObDASRowID &, double &) override
  {
    return OB_NOT_SUPPORTED;
  }
  int do_next_rowid(ObDASRowID &next_id, double &score) override
  {
    int ret = OB_SUCCESS;
    ++next_count_;
    if (!failure_delivered_ && pos_ == fail_after_) {
      failure_delivered_ = true;
      ret = OB_TIMEOUT;
    } else if (pos_ >= 8) {
      next_id.set_max();
      ret = OB_ITER_END;
    } else {
      next_id.set_uint64(++pos_);
      score = 0.0;
    }
    return ret;
  }

private:
  bool fail_open_;
  int64_t fail_after_;
  bool fail_rescan_;
  bool failure_delivered_;
  bool resource_opened_;
  int64_t pos_;
  int64_t open_count_;
  int64_t rescan_count_;
  int64_t next_count_;
};

static int fetch_filter_provider_test_partition(
    void *arg,
    const share::ObLSID &ls_id,
    const ObIArray<uint64_t> &index_table_ids,
    const ObIArray<ObTabletID> &index_tablet_ids,
    const ObTabletID &delete_tablet_id,
    const share::SCN &sql_read_snapshot,
    ObAsyncIndexPartitionSegCtx &partition)
{
  UNUSED(arg);
  UNUSED(ls_id);
  UNUSED(index_table_ids);
  UNUSED(index_tablet_ids);
  UNUSED(delete_tablet_id);
  int ret = OB_SUCCESS;
  const bool has_second_group = 2 == partition.main_tablet_id_.id();
  storage::ObAsyncIndexSegmentGroupCtx &storage_ctx = partition.storage_group_ctx_;
  for (int64_t index_idx = 0; OB_SUCC(ret) && index_idx < 2; ++index_idx) {
    ObSEArray<ObNewVersionRange, 3> ranges;
    const int64_t range_count = has_second_group ? (0 == index_idx ? 3 : 2) : 1;
    for (int64_t range_idx = 0; OB_SUCC(ret) && range_idx < range_count; ++range_idx) {
      ObNewVersionRange range;
      range.base_version_ = range_idx * 100;
      range.snapshot_version_ = (range_idx + 1) * 100;
      if (has_second_group && 1 == index_idx && 1 == range_idx) {
        range.snapshot_version_ = 300;
      }
      ret = ranges.push_back(range);
    }
    if (OB_SUCC(ret)) {
      // This mock fetch supplies catalog metadata, not storage iterators. Use
      // an in-place handle placeholder like the SegmentMerge mock fetcher:
      // copying a default ObTabletHandle is invalid without a tablet object.
      // Group alignment below still runs the real storage range algorithm.
      const int64_t published_version = has_second_group ? 300 : 100;
      if (OB_ISNULL(storage_ctx.index_tablet_handles_.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(storage_ctx.index_version_ranges_.push_back(ranges))) {
      } else if (OB_FAIL(storage_ctx.index_published_versions_.push_back(
                     published_version))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = partition.storage_group_ctx_.set_delete_table_version_ranges(
        ObSEArray<ObNewVersionRange, 1>(), 300);
  }
  if (OB_SUCC(ret)) {
    ret = partition.storage_group_ctx_.calc_query_version_ranges(sql_read_snapshot);
  }
  return ret;
}

static int build_filter_provider_test_catalog(ObDASAsyncIndexQueryCtx &catalog)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 2> index_ids;
  share::SCN snapshot;
  catalog.set_mock_fetcher(fetch_filter_provider_test_partition, nullptr);
  if (OB_FAIL(snapshot.convert_for_tx(1000))) {
  } else if (OB_FAIL(index_ids.push_back(9001))) {
  } else if (OB_FAIL(index_ids.push_back(9002))) {
  }
  for (int64_t partition_id = 1; OB_SUCC(ret) && partition_id <= 2; ++partition_id) {
    ObSEArray<ObTabletID, 2> index_tablets;
    if (OB_FAIL(index_tablets.push_back(ObTabletID(10000 + partition_id * 10)))) {
    } else if (OB_FAIL(index_tablets.push_back(ObTabletID(10001 + partition_id * 10)))) {
    } else if (OB_FAIL(catalog.add_partition(
                   share::ObLSID(1), ObTabletID(partition_id), index_ids, index_tablets,
                   ObTabletID(20000 + partition_id), snapshot))) {
    }
  }
  if (OB_SUCC(ret)) {
    ret = catalog.finalize();
  }
  return ret;
}

class TestFusionWork : public ObIDASFusionWork
{
public:
  TestFusionWork(const int64_t path_id,
                 const ObDASFusionWorkUnitKind kind,
                 const int execute_ret = OB_SUCCESS)
    : path_id_(path_id),
      kind_(kind),
      execute_ret_(execute_ret),
      execute_count_(0)
  {}

  ObDASFusionWorkUnitKind get_kind() const override { return kind_; }
  int64_t get_path_id() const override { return path_id_; }
  int execute(const int64_t,
              ObDASFusionParallelCoordinator &) override
  {
    ++execute_count_;
    return execute_ret_;
  }
  void destroy() override {}
  TO_STRING_KV(K_(path_id), K_(kind), K_(execute_ret), K_(execute_count));

  int64_t path_id_;
  ObDASFusionWorkUnitKind kind_;
  int execute_ret_;
  int64_t execute_count_;
};

class TestFailingFusionSliceProvider : public ObDASFusionSliceProvider
{
public:
  TestFailingFusionSliceProvider(const int runtime_estimate_ret,
                                 const int build_slices_ret)
    : runtime_estimate_ret_(runtime_estimate_ret),
      build_slices_ret_(build_slices_ret),
      runtime_refine_count_(0)
  {}

  ObDASFusionSliceType get_slice_type() const override
  {
    return ObDASFusionSliceType::DOCID_RANGE;
  }
  bool is_supported(const ObDASFusionPathType,
                    const ObDASSearchCtx &) const override
  {
    return true;
  }
  int estimate(
      const ObDASFusionLogicalPath &,
      const ObDASFusionCtDef &,
      const ObDASSearchCtx &search_ctx,
      ObDASFusionProviderWorkProfile &profile) const override
  {
    const ObDASSearchCost &table_row_count =
        search_ctx.get_row_count();
    profile.reset();
    profile.slice_type_ = get_slice_type();
    profile.is_supported_ = true;
    profile.table_row_count_ = table_row_count.is_valid()
        ? table_row_count.cost()
        : -1;
    profile.ror_op_count_ = 1;
    profile.ror_work_row_count_ = -1;
    return OB_SUCCESS;
  }
  int refine_runtime_work_estimate(
      const ObDASFusionLogicalPath &,
      ObDASFusionProviderWorkProfile &profile) const override
  {
    ++runtime_refine_count_;
    if (OB_SUCCESS == runtime_estimate_ret_) {
      profile.ror_op_count_ = 1;
      profile.ror_work_row_count_ =
          ObDASFusionDopPolicy::MIN_ROR_WORK_ROW_COUNT;
    }
    return runtime_estimate_ret_;
  }
  int build_slices(
      const ObDASFusionLogicalPath &,
      const int64_t,
      const ObDASFusionProviderWorkProfile &,
      ObDASSearchCtx &,
      common::ObIAllocator &,
      common::ObIArray<ObDASFusionWorkSlice> &) const override
  {
    return build_slices_ret_;
  }
  int bind_slice(const ObDASFusionWorkSlice &,
                 ObDASSearchCtx &) const override
  {
    return OB_SUCCESS;
  }
  int64_t get_runtime_refine_count() const
  {
    return runtime_refine_count_;
  }

private:
  int runtime_estimate_ret_;
  int build_slices_ret_;
  mutable int64_t runtime_refine_count_;
};

static int decide_docid_range_dop(
    const int64_t configured_query_dop,
    const bool is_supported,
    const int64_t table_row_count,
    const int64_t ror_op_count,
    const int64_t ror_work_row_count,
    ObDASFusionDopDecision &decision,
    const double tenant_cpu_pressure_percent = -1.0)
{
  ObDASFusionProviderWorkProfile profile;
  profile.slice_type_ = ObDASFusionSliceType::DOCID_RANGE;
  profile.is_supported_ = is_supported;
  profile.table_row_count_ = table_row_count;
  profile.ror_op_count_ = ror_op_count;
  profile.ror_work_row_count_ = ror_work_row_count;
  return ObDASFusionDopPolicy::decide(
      configured_query_dop, profile, tenant_cpu_pressure_percent,
      decision);
}

static void init_docid_range_slice(
    ObDASFusionPathSliceWork &work,
    const int64_t path_id,
    const int64_t slice_id,
    const int64_t path_top_k_limit)
{
  ObObj lo;
  ObObj hi;
  lo.set_min_value();
  hi.set_max_value();
  ASSERT_EQ(OB_SUCCESS,
            work.work_slice_.init_docid_range(
                path_id, slice_id, lo, hi, path_top_k_limit));
}

// ============================================================================
// Section 1: ObDASFusionMaterializedRow Tests
// ============================================================================

TEST(ObDASFusionMaterializedRowTest, default_constructor)
{
  ObDASFusionMaterializedRow row;
  EXPECT_EQ(0UL, row.rowkey_);
  EXPECT_DOUBLE_EQ(0.0, row.score_);
}

TEST(ObDASFusionMaterializedRowTest, parameterized_constructor)
{
  ObDASFusionMaterializedRow row(42, 3.14);
  EXPECT_EQ(42UL, row.rowkey_);
  EXPECT_DOUBLE_EQ(3.14, row.score_);
}

TEST(ObDASFusionMaterializedRowTest, boundary_values)
{
  ObDASFusionMaterializedRow row_max(UINT64_MAX, DBL_MAX);
  EXPECT_EQ(UINT64_MAX, row_max.rowkey_);
  EXPECT_DOUBLE_EQ(DBL_MAX, row_max.score_);

  ObDASFusionMaterializedRow row_zero(0, 0.0);
  EXPECT_EQ(0UL, row_zero.rowkey_);
  EXPECT_DOUBLE_EQ(0.0, row_zero.score_);

  ObDASFusionMaterializedRow row_neg(1, -1.0);
  EXPECT_DOUBLE_EQ(-1.0, row_neg.score_);
}

// ============================================================================
// 第 1.1 节：ObDASFusionWorkUnit 单测
// ============================================================================

TEST(ObDASFusionWorkUnitTest, default_constructor)
{
  ObDASFusionWorkUnit work_unit;
  EXPECT_EQ(-1, work_unit.unit_id_);
  EXPECT_EQ(-1, work_unit.path_id_);
  EXPECT_EQ(-1, work_unit.dependency_unit_id_);
  EXPECT_EQ(ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE,
            work_unit.kind_);
  EXPECT_EQ(nullptr, work_unit.work_);
  EXPECT_FALSE(work_unit.submitted_);
  EXPECT_FALSE(work_unit.finished_);
  EXPECT_EQ(OB_SUCCESS, work_unit.err_code_);
  EXPECT_EQ(nullptr, work_unit.profile_);
  EXPECT_EQ(0, work_unit.enqueue_ts_us_);
  EXPECT_EQ(0, work_unit.dequeue_ts_us_);
  EXPECT_EQ(0, work_unit.work_begin_ts_us_);
  EXPECT_EQ(0, work_unit.work_end_ts_us_);
  EXPECT_EQ(0, work_unit.complete_ts_us_);
}

TEST(ObDASFusionWorkUnitTest, init_validation_and_success)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObDASFusionWorkUnit work_unit;
  TestFusionWork work(
      2, ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            work_unit.init(-1, 2, work));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            work_unit.init(0, -1, work));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            work_unit.init(0, 0, work));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            work_unit.init(0, 2, work, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            work_unit.init(1, 2, work, -2));

  work_unit.enqueue_ts_us_ = 1;
  work_unit.dequeue_ts_us_ = 2;
  work_unit.work_begin_ts_us_ = 3;
  work_unit.work_end_ts_us_ = 4;
  work_unit.complete_ts_us_ = 5;
  ASSERT_EQ(OB_SUCCESS,
            work_unit.init(3, 2, work, 1));
  EXPECT_EQ(3, work_unit.unit_id_);
  EXPECT_EQ(2, work_unit.path_id_);
  EXPECT_EQ(1, work_unit.dependency_unit_id_);
  EXPECT_EQ(ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE,
            work_unit.kind_);
  EXPECT_EQ(&work, work_unit.work_);
  EXPECT_FALSE(work_unit.submitted_);
  EXPECT_FALSE(work_unit.finished_);
  EXPECT_EQ(OB_SUCCESS, work_unit.err_code_);
  EXPECT_EQ(0, work_unit.enqueue_ts_us_);
  EXPECT_EQ(0, work_unit.dequeue_ts_us_);
  EXPECT_EQ(0, work_unit.work_begin_ts_us_);
  EXPECT_EQ(0, work_unit.work_end_ts_us_);
  EXPECT_EQ(0, work_unit.complete_ts_us_);
}

TEST(ObDASFusionWorkUnitTest, prepare_work_has_no_slice_state)
{
  ObDASFusionBitmapPrepareWork prepare_work;
  prepare_work.exec_env_.path_idx_ = 1;
  ObDASFusionWorkUnit work_unit;
  ASSERT_EQ(OB_SUCCESS,
            work_unit.init(0, 1, prepare_work));
  EXPECT_EQ(ObDASFusionWorkUnitKind::PREPARE,
            work_unit.kind_);
  EXPECT_EQ(&prepare_work, work_unit.work_);
}

TEST(ObDASFusionWorkSliceTest, whole_path_and_docid_range_are_distinct)
{
  ObDASFusionWorkSlice whole_path;
  ASSERT_EQ(OB_SUCCESS, whole_path.init_whole_path(2));
  EXPECT_TRUE(whole_path.is_valid());
  EXPECT_FALSE(whole_path.is_partitioned());
  EXPECT_EQ(ObDASFusionSliceType::WHOLE_PATH, whole_path.type_);
  EXPECT_EQ(2, whole_path.path_id_);
  EXPECT_EQ(0, whole_path.slice_id_);

  ObObj lo;
  ObObj hi;
  lo.set_uint64(100);
  hi.set_uint64(199);
  ObDASFusionWorkSlice docid_range;
  ASSERT_EQ(OB_SUCCESS,
            docid_range.init_docid_range(2, 3, lo, hi, 50));
  EXPECT_TRUE(docid_range.is_partitioned());
  EXPECT_TRUE(docid_range.is_docid_range());
  EXPECT_EQ(ObDASFusionSliceType::DOCID_RANGE, docid_range.type_);
  EXPECT_EQ(3, docid_range.slice_id_);
  EXPECT_EQ(50, docid_range.path_top_k_limit_);
  EXPECT_EQ(100UL, docid_range.docid_range_.lo_.get_uint64());
  EXPECT_EQ(199UL, docid_range.docid_range_.hi_.get_uint64());

  ObDASFusionWorkSlice no_limit_docid_range;
  ASSERT_EQ(OB_SUCCESS,
            no_limit_docid_range.init_docid_range(
                2, 4, lo, hi, -1));
  EXPECT_EQ(-1, no_limit_docid_range.path_top_k_limit_);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            no_limit_docid_range.init_docid_range(
                2, 4, lo, hi, -2));
}

namespace
{
void append_storage_split(ObArrayArray<ObStoreRange> &splits,
                          ObObj &start, ObObj &end, const uint8_t flags)
{
  ObStoreRange range;
  ASSERT_EQ(OB_SUCCESS, range.get_start_key().assign(&start, 1));
  ASSERT_EQ(OB_SUCCESS, range.get_end_key().assign(&end, 1));
  range.get_border_flag().set_data(flags);
  ObSEArray<ObStoreRange, 1> task;
  ASSERT_EQ(OB_SUCCESS, task.push_back(range));
  ASSERT_EQ(OB_SUCCESS, splits.push_back(task));
}

class TestConvertedRangeProvider : public TestFailingFusionSliceProvider
{
public:
  explicit TestConvertedRangeProvider(const ObArrayArray<ObStoreRange> &splits)
    : TestFailingFusionSliceProvider(OB_SUCCESS, OB_SUCCESS), splits_(splits)
  {}

  int build_slices(const ObDASFusionLogicalPath &path,
                   const int64_t,
                   const ObDASFusionProviderWorkProfile &,
                   ObDASSearchCtx &,
                   ObIAllocator &,
                   ObIArray<ObDASFusionWorkSlice> &slices) const override
  {
    int ret = OB_SUCCESS;
    ObSEArray<ObObj, 8> lo;
    ObSEArray<ObObj, 8> hi;
    if (OB_FAIL(ObDASFusionDocIdRangeSliceProvider::convert_split_ranges(splits_, lo, hi))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < lo.count(); ++i) {
        ObDASFusionWorkSlice slice;
        if (OB_FAIL(slice.init_docid_range(path.path_id_, i, lo.at(i), hi.at(i),
                                         path.path_top_k_limit_))) {
        } else if (OB_FAIL(slices.push_back(slice))) {
        }
      }
    }
    return ret;
  }

private:
  const ObArrayArray<ObStoreRange> &splits_;
};
} // namespace

TEST(ObDASFusionWorkSliceTest, rejects_reversed_bounds_but_keeps_singletons_and_sentinels)
{
  ObObj lo;
  ObObj hi;
  ObDASFusionWorkSlice slice;
  lo.set_uint64(1025);
  hi.set_uint64(1024);
  EXPECT_EQ(OB_INVALID_ARGUMENT, slice.init_docid_range(0, 0, lo, hi, 100));
  lo.set_uint64(1024);
  EXPECT_EQ(OB_SUCCESS, slice.init_docid_range(0, 0, lo, hi, 100));
  lo.set_min_value();
  EXPECT_EQ(OB_SUCCESS, slice.init_docid_range(0, 0, lo, hi, 100));
  hi.set_max_value();
  EXPECT_EQ(OB_SUCCESS, slice.init_docid_range(0, 0, lo, hi, 100));
  lo.set_uint64(UINT64_MAX);
  EXPECT_EQ(OB_SUCCESS, slice.init_docid_range(0, 0, lo, hi, 100));
}

TEST(ObDASFusionSliceProviderTest, converts_open_closed_docid_boundaries)
{
  struct RangeCase {
    uint64_t start_;
    uint64_t end_;
    uint8_t flags_;
    bool empty_;
    uint64_t lo_;
    uint64_t hi_;
  };
  const RangeCase cases[] = {
    {1024, 1024, 2, true, 0, 0},
    {1024, 1025, 0, true, 0, 0},
    {1024, 1024, 3, false, 1024, 1024},
    {1024, 1026, 0, false, 1025, 1025},
    {1024, 1026, 1, false, 1024, 1025},
    {1024, 1026, 2, false, 1025, 1026},
    {1024, 1026, 3, false, 1024, 1026},
    {0, 0, 3, false, 0, 0},
    {UINT64_MAX, UINT64_MAX, 3, false, UINT64_MAX, UINT64_MAX},
  };
  for (const RangeCase &c : cases) {
    SCOPED_TRACE(c.flags_);
    ObObj start;
    ObObj end;
    start.set_uint64(c.start_);
    end.set_uint64(c.end_);
    ObArrayArray<ObStoreRange> splits;
    append_storage_split(splits, start, end, c.flags_);
    ObSEArray<ObObj, 8> lo;
    ObSEArray<ObObj, 8> hi;
    ASSERT_EQ(OB_SUCCESS, ObDASFusionDocIdRangeSliceProvider::convert_split_ranges(splits, lo, hi));
    ASSERT_EQ(c.empty_ ? 0 : 1, lo.count());
    ASSERT_EQ(lo.count(), hi.count());
    if (!c.empty_) {
      EXPECT_EQ(c.lo_, lo.at(0).get_uint64());
      EXPECT_EQ(c.hi_, hi.at(0).get_uint64());
    }
  }
}

TEST(ObDASFusionSliceProviderTest, filters_empty_splits_without_losing_adjacent_docids)
{
  ObObj keys[4];
  keys[0].set_min_value();
  keys[1].set_uint64(1024);
  keys[2].set_uint64(2048);
  keys[3].set_max_value();
  ObArrayArray<ObStoreRange> splits;
  append_storage_split(splits, keys[1], keys[1], 2);
  append_storage_split(splits, keys[0], keys[1], 2);
  append_storage_split(splits, keys[1], keys[1], 2);
  append_storage_split(splits, keys[1], keys[2], 2);
  append_storage_split(splits, keys[2], keys[2], 2);
  append_storage_split(splits, keys[2], keys[3], 0);
  append_storage_split(splits, keys[2], keys[2], 2);
  ObSEArray<ObObj, 8> lo;
  ObSEArray<ObObj, 8> hi;
  ASSERT_EQ(OB_SUCCESS, ObDASFusionDocIdRangeSliceProvider::convert_split_ranges(splits, lo, hi));
  ASSERT_EQ(3, lo.count());
  ASSERT_EQ(3, hi.count());
  EXPECT_TRUE(lo.at(0).is_min_value());
  EXPECT_EQ(1024UL, hi.at(0).get_uint64());
  EXPECT_EQ(1025UL, lo.at(1).get_uint64());
  EXPECT_EQ(2048UL, hi.at(1).get_uint64());
  EXPECT_EQ(2049UL, lo.at(2).get_uint64());
  EXPECT_TRUE(hi.at(2).is_max_value());
}

TEST(ObDASFusionSliceProviderTest, preserves_whole_range_and_rejects_invalid_boundaries)
{
  for (int i = 0; i < 5; ++i) {
    ObObj start;
    ObObj end;
    start.set_min_value();
    end.set_max_value();
    if (i == 1) {
      start.set_uint64(UINT64_MAX); // Exclusive start still reports overflow.
    } else if (i == 2) {
      end.set_uint64(0); // Exclusive end still reports underflow.
    } else if (i == 3) {
      start.set_int(1);
    } else if (i == 4) {
      end.set_int(1);
    }
    ObArrayArray<ObStoreRange> splits;
    append_storage_split(splits, start, end, 0);
    ObSEArray<ObObj, 8> lo;
    ObSEArray<ObObj, 8> hi;
    const int ret = ObDASFusionDocIdRangeSliceProvider::convert_split_ranges(splits, lo, hi);
    if (i == 0) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(1, lo.count());
      ASSERT_EQ(1, hi.count());
      EXPECT_TRUE(lo.at(0).is_min_value());
      EXPECT_TRUE(hi.at(0).is_max_value());
    } else {
      EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
      EXPECT_EQ(0, lo.count());
      EXPECT_EQ(0, hi.count());
    }
  }
}

TEST(ObDASFusionSliceProviderTest, distinguishes_no_splits_from_invalid_empty_task)
{
  ObArrayArray<ObStoreRange> splits;
  ObSEArray<ObObj, 8> lo;
  ObSEArray<ObObj, 8> hi;
  EXPECT_EQ(OB_SUCCESS, ObDASFusionDocIdRangeSliceProvider::convert_split_ranges(splits, lo, hi));
  EXPECT_EQ(0, lo.count());
  EXPECT_EQ(0, hi.count());
  ObSEArray<ObStoreRange, 1> empty_task;
  ASSERT_EQ(OB_SUCCESS, splits.push_back(empty_task));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObDASFusionDocIdRangeSliceProvider::convert_split_ranges(splits, lo, hi));
}

TEST(ObDASSearchCtxTest, pool_binding_survives_mtl_destroy_until_reset)
{
  typedef storage::ObAsyncInvIteratorPool Pool;
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("TestPoolBinding");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  Pool *pool = OB_NEW(Pool, ObMemAttr(MTL_ID(), "TestInvPool"));
  ASSERT_NE(nullptr, pool);
  share::ObTenantBase *tenant = share::ObTenantEnv::get_tenant_local();
  Pool *original = tenant->get<Pool *>();
  tenant->set<Pool *>(nullptr);
  search_ctx.enable_inv_iter_pool_ = true;
  blocksstable::ObInvPostingIterEntry *entry = nullptr;
  EXPECT_EQ(OB_SUCCESS, search_ctx.acquire_posting_entry(entry));
  EXPECT_EQ(nullptr, entry);
  EXPECT_EQ(nullptr, search_ctx.inv_iter_pool_);
  tenant->set<Pool *>(pool);
  EXPECT_EQ(OB_SUCCESS, search_ctx.acquire_posting_entry(entry));
  EXPECT_EQ(nullptr, search_ctx.inv_iter_pool_);
  EXPECT_EQ(1, pool->refs_);
  EXPECT_EQ(OB_SUCCESS, pool->init());
  EXPECT_EQ(OB_SUCCESS, search_ctx.acquire_posting_entry(entry));
  EXPECT_EQ(pool, search_ctx.inv_iter_pool_);
  // Any Entry lease is independent of the query binding.
  if (nullptr != entry) {
    pool->release_inv_entry(entry);
  }
  EXPECT_EQ(2, pool->refs_);
  Pool *owner = pool;
  tenant->set<Pool *>(original);
  Pool::mtl_destroy(pool);
  EXPECT_EQ(nullptr, pool);
  EXPECT_EQ(1, owner->refs_);
  EXPECT_FALSE(owner->pin());
  EXPECT_EQ(OB_SUCCESS, search_ctx.acquire_posting_entry(entry));
  EXPECT_EQ(nullptr, entry);
  EXPECT_EQ(1, owner->refs_);
  search_ctx.reset();
  EXPECT_EQ(nullptr, search_ctx.inv_iter_pool_);
  search_ctx.reset();
}

TEST(ObDASFusionPathAdapterTest, filtered_splits_keep_dense_ids_and_whole_path_fallback)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("SplitFallback");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  ObDASFusionCtDef fusion_ctdef(alloc);
  fusion_ctdef.has_search_subquery_ = true;
  ObDASFusionWholePathSliceProvider whole_path;
  ObObj keys[3];
  keys[0].set_min_value();
  keys[1].set_uint64(1024);
  keys[2].set_max_value();
  for (int valid_count = 0; valid_count <= 2; ++valid_count) {
    ObArrayArray<ObStoreRange> splits;
    append_storage_split(splits, keys[1], keys[1], 2);
    if (valid_count >= 1) {
      append_storage_split(splits, keys[0], keys[1], 2);
    }
    append_storage_split(splits, keys[1], keys[1], 2);
    if (valid_count == 2) {
      append_storage_split(splits, keys[1], keys[2], 0);
    }
    TestConvertedRangeProvider provider(splits);
    ObDASFusionPathAdapter adapter;
    adapter.logical_path_.path_id_ = 0;
    adapter.logical_path_.type_ = ObDASFusionPathType::QUERY;
    adapter.logical_path_.path_top_k_limit_ = 100;
    ASSERT_EQ(OB_SUCCESS, adapter.add_candidate_provider(provider));
    ASSERT_EQ(OB_SUCCESS, adapter.add_candidate_provider(whole_path));
    ASSERT_EQ(OB_SUCCESS, adapter.plan_slices(fusion_ctdef, search_ctx, 4, 0.0, alloc));
    ASSERT_EQ(valid_count < 2 ? 1 : 2, adapter.get_slice_count());
    ASSERT_NE(nullptr, adapter.get_selected_provider());
    EXPECT_EQ(valid_count < 2 ? ObDASFusionSliceType::WHOLE_PATH : ObDASFusionSliceType::DOCID_RANGE,
              adapter.get_selected_provider()->get_slice_type());
    for (int64_t i = 0; i < adapter.get_slice_count(); ++i) {
      ASSERT_NE(nullptr, adapter.get_slice(i));
      EXPECT_EQ(i, adapter.get_slice(i)->slice_id_);
    }
  }
}

TEST(ObDASFusionLogicalPathTest, validates_top_k_limit_at_path_boundary)
{
  ObDASFusionLogicalPath logical_path;
  ObDASFusionRtDef rtdef;
  int dummy_iter = 0;
  ObDASIter *child_iter = reinterpret_cast<ObDASIter *>(&dummy_iter);

  EXPECT_EQ(OB_SUCCESS,
            logical_path.init(
                0, ObDASFusionPathType::QUERY,
                &rtdef, child_iter, -1));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            logical_path.init(
                0, ObDASFusionPathType::QUERY,
                &rtdef, child_iter, -2));
}

TEST(ObDASFusionSliceProviderTest, async_query_supports_docid_range)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("SliceProvider");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
  ObDASFusionDocIdRangeSliceProvider provider;

  EXPECT_TRUE(provider.is_supported(
      ObDASFusionPathType::QUERY, search_ctx));
  EXPECT_FALSE(provider.is_supported(
      ObDASFusionPathType::KNN, search_ctx));

  ObDASAsyncIndexQueryCtx async_query_ctx;
  search_ctx.set_async_query_ctx(&async_query_ctx);
  EXPECT_TRUE(provider.is_supported(
      ObDASFusionPathType::QUERY, search_ctx));

  search_ctx.set_async_query_ctx(nullptr);
  ExprFixedArray nested_rowid_exprs(alloc);
  search_ctx.scopes_[DAS_SCOPE_NESTED_SYNC].rowid_exprs_ =
      &nested_rowid_exprs;
  EXPECT_FALSE(provider.is_supported(
      ObDASFusionPathType::QUERY, search_ctx));
}

TEST(ObDASFusionSliceProviderTest,
     whole_path_estimate_uses_runtime_table_row_count)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("ProviderEst");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  ObDASFusionCtDef fusion_ctdef(alloc);
  ObDASFusionLogicalPath logical_path;
  ObDASFusionProviderWorkProfile profile;
  ObDASFusionWholePathSliceProvider whole_path_provider;
  const int64_t table_row_count = 123456;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
  search_ctx.table_row_count_ = ObDASSearchCost(table_row_count);
  fusion_ctdef.has_search_subquery_ = true;

  ASSERT_EQ(OB_SUCCESS,
            whole_path_provider.estimate(
                logical_path, fusion_ctdef, search_ctx, profile));
  EXPECT_EQ(table_row_count, profile.table_row_count_);

  search_ctx.table_row_count_.reset();
  ASSERT_EQ(OB_SUCCESS,
            whole_path_provider.estimate(
                logical_path, fusion_ctdef, search_ctx, profile));
  EXPECT_EQ(-1, profile.table_row_count_);
}

TEST(ObDASFusionSliceProviderTest,
     query_docid_range_allows_missing_top_k)
{
  ObArenaAllocator alloc("MissingTopK");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  ObDASFusionCtDef fusion_ctdef(alloc);
  ObDASFusionDocIdRangeSliceProvider docid_range_provider;
  ObDASFusionProviderWorkProfile profile;
  int dummy_iter = 0;
  ObDASFusionRtDef rtdef;
  ObDASFusionLogicalPath logical_path;

  ASSERT_EQ(OB_SUCCESS,
            logical_path.init(
                0, ObDASFusionPathType::QUERY, &rtdef,
                reinterpret_cast<ObDASIter *>(&dummy_iter), -1));
  EXPECT_EQ(-1, logical_path.path_top_k_limit_);

  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
  fusion_ctdef.has_search_subquery_ = true;
  ASSERT_EQ(OB_SUCCESS,
            docid_range_provider.estimate(
                logical_path, fusion_ctdef, search_ctx, profile));
  EXPECT_TRUE(profile.is_supported_);
}

TEST(ObDASFusionPathAdapterTest, async_query_dop_one_uses_whole_path)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("PathAdapter");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
  ObDASAsyncIndexQueryCtx async_query_ctx;
  search_ctx.set_async_query_ctx(&async_query_ctx);

  ObDASFusionCtDef fusion_ctdef(alloc);
  fusion_ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  fusion_ctdef.children_cnt_ = 1;
  fusion_ctdef.has_search_subquery_ = true;
  fusion_ctdef.query_dop_ = 1;

  ObDASFusionWholePathSliceProvider whole_path_provider;
  ObDASFusionDocIdRangeSliceProvider docid_range_provider;
  ObDASFusionPathAdapter path_adapter;
  path_adapter.logical_path_.path_id_ = 0;
  path_adapter.logical_path_.type_ = ObDASFusionPathType::QUERY;
  path_adapter.logical_path_.path_top_k_limit_ = 100;
  ASSERT_EQ(OB_SUCCESS,
            path_adapter.add_candidate_provider(
                docid_range_provider));
  ASSERT_EQ(OB_SUCCESS,
            path_adapter.add_candidate_provider(
                whole_path_provider));

  ASSERT_EQ(OB_SUCCESS,
            path_adapter.plan_slices(
                fusion_ctdef, search_ctx,
                fusion_ctdef.query_dop_,
                -1.0, alloc));
  ASSERT_NE(nullptr, path_adapter.get_selected_provider());
  EXPECT_EQ(ObDASFusionSliceType::WHOLE_PATH,
            path_adapter.get_selected_provider()->get_slice_type());
  EXPECT_EQ(1, path_adapter.get_slice_count());
  EXPECT_EQ(1,
            path_adapter.get_dop_decision()
                .actual_path_slice_count_);
  ASSERT_NE(nullptr, path_adapter.get_slice(0));
  EXPECT_EQ(100, path_adapter.get_slice(0)->path_top_k_limit_);
}

TEST(ObDASFusionPathAdapterTest,
     explicit_dop_uses_whole_path_for_async_segment_local_range)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("SegmentBitmap");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  ObDASFusionCtDef fusion_ctdef(alloc);
  ObDASFusionRtDef rtdef;
  TestEstimatedWorkSearchOp range(search_ctx, -1);
  TestEstimatedWorkSearchOp bitmap(search_ctx, -1);
  TestEstimatedWorkSearchOp segment_merge(search_ctx, -1);
  ObIDASSearchOp *bitmap_children[] = {&range};
  ObIDASSearchOp *segment_children[] = {&bitmap};
  ObDASSearchDriverIter search_iter;
  ObDASFusionWholePathSliceProvider whole_path_provider;
  ObDASFusionDocIdRangeSliceProvider docid_range_provider;
  ObDASFusionPathAdapter path_adapter;
  ObDASFusionProviderWorkProfile profile;

  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
  fusion_ctdef.has_search_subquery_ = true;
  fusion_ctdef.query_dop_ = 4;
  range.set_work_role(DAS_SEARCH_OP_ASYNC_SCALAR_RANGE, false);
  bitmap.set_work_role(DAS_SEARCH_OP_BITMAP, false);
  bitmap.set_children(bitmap_children, ARRAYSIZEOF(bitmap_children));
  segment_merge.set_work_role(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, false);
  segment_merge.set_children(segment_children, ARRAYSIZEOF(segment_children));
  search_iter.set_root_op(&segment_merge);
  ASSERT_EQ(OB_SUCCESS, path_adapter.init(
      0, ObDASFusionPathType::QUERY, &rtdef, &search_iter, 100));

  ASSERT_EQ(OB_SUCCESS, docid_range_provider.estimate(
      path_adapter.get_logical_path(), fusion_ctdef, search_ctx, profile));
  EXPECT_FALSE(profile.is_supported_);
  ASSERT_EQ(OB_SUCCESS, path_adapter.add_candidate_provider(docid_range_provider));
  ASSERT_EQ(OB_SUCCESS, path_adapter.add_candidate_provider(whole_path_provider));
  ASSERT_EQ(OB_SUCCESS, path_adapter.plan_slices(
      fusion_ctdef, search_ctx, fusion_ctdef.query_dop_, 0.0, alloc));

  ASSERT_NE(nullptr, path_adapter.get_selected_provider());
  EXPECT_EQ(ObDASFusionSliceType::WHOLE_PATH,
            path_adapter.get_selected_provider()->get_slice_type());
  EXPECT_EQ(1, path_adapter.get_dop_decision().target_path_dop_);
  EXPECT_EQ(1, path_adapter.get_slice_count());
}

TEST(ObDASFusionPathAdapterTest,
     auto_planning_failure_requests_whole_fusion_serial_fallback)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("PathFallback");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  ObDASFusionCtDef fusion_ctdef(alloc);
  ObDASFusionWholePathSliceProvider whole_path_provider;
  fusion_ctdef.has_search_subquery_ = true;
  search_ctx.table_row_count_ = ObDASSearchCost(
      ObDASFusionDopPolicy::MIN_TABLE_ROW_COUNT);

  const TestFailingFusionSliceProvider failing_providers[] = {
      TestFailingFusionSliceProvider(
          OB_ERR_UNEXPECTED, OB_SUCCESS),
      TestFailingFusionSliceProvider(
          OB_SUCCESS, OB_ERR_UNEXPECTED),
  };
  for (const TestFailingFusionSliceProvider &failing_provider
       : failing_providers) {
    ObDASFusionPathAdapter path_adapter;
    path_adapter.logical_path_.path_id_ = 0;
    path_adapter.logical_path_.type_ = ObDASFusionPathType::QUERY;
    path_adapter.logical_path_.path_top_k_limit_ = 100;
    ASSERT_EQ(OB_SUCCESS,
              path_adapter.add_candidate_provider(
                  failing_provider));
    ASSERT_EQ(OB_SUCCESS,
              path_adapter.add_candidate_provider(
                  whole_path_provider));

    EXPECT_EQ(OB_SUCCESS,
              path_adapter.plan_slices(
                  fusion_ctdef, search_ctx,
                  ObDASFusionDopPolicy::AUTO_QUERY_DOP,
                  0.0, alloc));
    EXPECT_TRUE(path_adapter.should_force_fusion_serial());
    EXPECT_EQ(nullptr, path_adapter.get_selected_provider());
    EXPECT_EQ(0, path_adapter.get_slice_count());
    EXPECT_EQ(1, path_adapter.get_dop_decision().target_path_dop_);
    EXPECT_EQ(1,
              path_adapter.get_dop_decision()
                  .actual_path_slice_count_);
  }
}

TEST(ObDASFusionPathAdapterTest,
     auto_small_table_skips_runtime_work_refinement)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("PathAdmission");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  ObDASFusionCtDef fusion_ctdef(alloc);
  TestFailingFusionSliceProvider candidate_provider(
      OB_ERR_UNEXPECTED, OB_SUCCESS);
  ObDASFusionWholePathSliceProvider whole_path_provider;
  ObDASFusionPathAdapter path_adapter;
  fusion_ctdef.has_search_subquery_ = true;
  search_ctx.table_row_count_ = ObDASSearchCost(
      ObDASFusionDopPolicy::MIN_TABLE_ROW_COUNT - 1);
  path_adapter.logical_path_.path_id_ = 0;
  path_adapter.logical_path_.type_ = ObDASFusionPathType::QUERY;
  path_adapter.logical_path_.path_top_k_limit_ = 100;
  ASSERT_EQ(OB_SUCCESS,
            path_adapter.add_candidate_provider(candidate_provider));
  ASSERT_EQ(OB_SUCCESS,
            path_adapter.add_candidate_provider(whole_path_provider));

  ASSERT_EQ(OB_SUCCESS,
            path_adapter.plan_slices(
                fusion_ctdef, search_ctx,
                ObDASFusionDopPolicy::AUTO_QUERY_DOP,
                0.0, alloc));
  EXPECT_EQ(0, candidate_provider.get_runtime_refine_count());
  EXPECT_EQ(ObDASFusionAdmissionResult::TABLE_TOO_SMALL,
            path_adapter.get_dop_decision().admission_result_);
  ASSERT_NE(nullptr, path_adapter.get_selected_provider());
  EXPECT_EQ(ObDASFusionSliceType::WHOLE_PATH,
            path_adapter.get_selected_provider()->get_slice_type());
}

TEST(ObDASFusionPathAdapterTest,
     explicit_dop_planning_failure_is_not_swallowed)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("PathFailure");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  ObDASFusionCtDef fusion_ctdef(alloc);
  ObDASFusionWholePathSliceProvider whole_path_provider;
  TestFailingFusionSliceProvider failing_provider(
      OB_SUCCESS, OB_ERR_UNEXPECTED);
  ObDASFusionPathAdapter path_adapter;
  fusion_ctdef.has_search_subquery_ = true;
  path_adapter.logical_path_.path_id_ = 0;
  path_adapter.logical_path_.type_ = ObDASFusionPathType::QUERY;
  path_adapter.logical_path_.path_top_k_limit_ = 100;
  ASSERT_EQ(OB_SUCCESS,
            path_adapter.add_candidate_provider(failing_provider));
  ASSERT_EQ(OB_SUCCESS,
            path_adapter.add_candidate_provider(whole_path_provider));

  EXPECT_EQ(OB_ERR_UNEXPECTED,
            path_adapter.plan_slices(
                fusion_ctdef, search_ctx, 4, 0.0, alloc));
  EXPECT_FALSE(path_adapter.should_force_fusion_serial());
}

TEST(ObDASFusionParallelCtxShapeTest,
     fallback_only_for_single_whole_path_slice)
{
  ObDASFusionParallelCtx parallel_ctx;
  ObDASFusionPathAdapter path_adapter;
  ObDASFusionPathAdapter second_path_adapter;
  ObDASFusionWholePathSliceProvider whole_path_provider;
  ObDASFusionDocIdRangeSliceProvider docid_range_provider;
  ObDASFusionWorkSlice whole_path_slice;
  ObDASFusionWorkSlice docid_range_slice;
  ObDASFusionWorkSlice second_whole_path_slice;
  ObObj docid_lo;
  ObObj docid_hi;

  EXPECT_FALSE(parallel_ctx.should_fallback_to_serial());

  parallel_ctx.force_serial_ = true;
  EXPECT_TRUE(parallel_ctx.should_fallback_to_serial());
  parallel_ctx.force_serial_ = false;

  ASSERT_EQ(OB_SUCCESS, whole_path_slice.init_whole_path(0));
  path_adapter.selected_provider_ = &whole_path_provider;
  ASSERT_EQ(OB_SUCCESS,
            path_adapter.work_slices_.push_back(whole_path_slice));
  ASSERT_EQ(OB_SUCCESS,
            parallel_ctx.path_adapters_.push_back(&path_adapter));
  EXPECT_TRUE(parallel_ctx.should_fallback_to_serial());

  path_adapter.selected_provider_ = &docid_range_provider;
  EXPECT_FALSE(parallel_ctx.should_fallback_to_serial());
  path_adapter.selected_provider_ = &whole_path_provider;

  docid_lo.set_uint64(0);
  docid_hi.set_uint64(99);
  ASSERT_EQ(OB_SUCCESS,
            docid_range_slice.init_docid_range(
                0, 0, docid_lo, docid_hi, 100));
  path_adapter.work_slices_.at(0) = docid_range_slice;
  EXPECT_FALSE(parallel_ctx.should_fallback_to_serial());
  path_adapter.work_slices_.at(0) = whole_path_slice;

  ASSERT_EQ(OB_SUCCESS, second_whole_path_slice.init_whole_path(0));
  ASSERT_EQ(OB_SUCCESS,
            path_adapter.work_slices_.push_back(
                second_whole_path_slice));
  EXPECT_FALSE(parallel_ctx.should_fallback_to_serial());
  path_adapter.work_slices_.pop_back();

  second_path_adapter.selected_provider_ = &whole_path_provider;
  ASSERT_EQ(OB_SUCCESS,
            second_path_adapter.work_slices_.push_back(
                second_whole_path_slice));
  ASSERT_EQ(OB_SUCCESS,
            parallel_ctx.path_adapters_.push_back(
                &second_path_adapter));
  EXPECT_FALSE(parallel_ctx.should_fallback_to_serial());
}

// ============================================================================
// 第 1.2 节：ObDASFusionDopPolicy 单测
// ============================================================================

TEST(ObDASFusionDopPolicyTest, reject_invalid_argument)
{
  ObDASFusionDopDecision decision;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            decide_docid_range_dop(
                -1, true, 1000000, 1, 100000,
                decision, 0.0));
}

TEST(ObDASFusionDopPolicyTest,
     explicit_query_dop_controls_range_task_count)
{
  ObDASFusionDopDecision decision;
  ASSERT_EQ(OB_SUCCESS,
            decide_docid_range_dop(
                4, true, 1, 0, -1,
                decision, 100.0));
  EXPECT_EQ(ObDASFusionDopMode::EXPLICIT, decision.mode_);
  EXPECT_EQ(4, decision.target_path_dop_);

  ASSERT_EQ(OB_SUCCESS,
            decide_docid_range_dop(
                4, false, 1000000, 1, 100000,
                decision, 0.0));
  EXPECT_EQ(1, decision.target_path_dop_);
}

TEST(ObDASFusionDopPolicyTest, auto_uses_simple_admission_rules)
{
  struct TestCase
  {
    bool is_supported_;
    int64_t table_row_count_;
    int64_t ror_op_count_;
    int64_t ror_work_row_count_;
    ObDASFusionAdmissionResult expected_result_;
    int64_t expected_dop_;
  };
  const TestCase test_cases[] = {
      {false, 1000000, 1, 100000,
       ObDASFusionAdmissionResult::UNSUPPORTED, 1},
      {true, -1, 1, 100000,
       ObDASFusionAdmissionResult::TABLE_ROW_COUNT_UNKNOWN, 1},
      {true, ObDASFusionDopPolicy::MIN_TABLE_ROW_COUNT - 1,
       1, 100000,
       ObDASFusionAdmissionResult::TABLE_TOO_SMALL, 1},
      {true, ObDASFusionDopPolicy::MIN_TABLE_ROW_COUNT,
       0, 0,
       ObDASFusionAdmissionResult::NO_ROR_OP, 1},
      {true, ObDASFusionDopPolicy::MIN_TABLE_ROW_COUNT,
       1, -1,
       ObDASFusionAdmissionResult::ROR_WORK_UNKNOWN, 1},
      {true, ObDASFusionDopPolicy::MIN_TABLE_ROW_COUNT,
       1, ObDASFusionDopPolicy::MIN_ROR_WORK_ROW_COUNT - 1,
       ObDASFusionAdmissionResult::ROR_WORK_TOO_SMALL, 1},
      {true, ObDASFusionDopPolicy::MIN_TABLE_ROW_COUNT,
       1, ObDASFusionDopPolicy::MIN_ROR_WORK_ROW_COUNT,
       ObDASFusionAdmissionResult::ADMITTED, 4},
  };
  for (const TestCase &test_case : test_cases) {
    ObDASFusionDopDecision decision;
    ASSERT_EQ(OB_SUCCESS,
              decide_docid_range_dop(
                  ObDASFusionDopPolicy::AUTO_QUERY_DOP,
                  test_case.is_supported_,
                  test_case.table_row_count_,
                  test_case.ror_op_count_,
                  test_case.ror_work_row_count_,
                  decision, 0.0));
    EXPECT_EQ(ObDASFusionDopMode::AUTO, decision.mode_);
    EXPECT_EQ(test_case.expected_result_,
              decision.admission_result_);
    EXPECT_EQ(test_case.expected_dop_,
              decision.target_path_dop_);
  }
}

TEST(ObDASFusionDopPolicyTest,
     auto_dop_uses_cpu_load_bands_and_fails_closed)
{
  struct TestCase
  {
    double tenant_cpu_pressure_percent_;
    int64_t expected_dop_;
  };
  const TestCase test_cases[] = {
      {-1.0, 1},
      {std::numeric_limits<double>::quiet_NaN(), 1},
      {std::numeric_limits<double>::infinity(), 1},
      {0.0, 4},
      {39.9, 4},
      {40.0, 2},
      {59.9, 2},
      {60.0, 1},
      {78.0, 1},
      {100.0, 1},
  };
  for (const TestCase &test_case : test_cases) {
    ObDASFusionDopDecision decision;
    ASSERT_EQ(OB_SUCCESS,
              decide_docid_range_dop(
                  ObDASFusionDopPolicy::AUTO_QUERY_DOP,
                  true,
                  ObDASFusionDopPolicy::MIN_TABLE_ROW_COUNT,
                  1,
                  ObDASFusionDopPolicy::MIN_ROR_WORK_ROW_COUNT,
                  decision,
                  test_case.tenant_cpu_pressure_percent_));
    EXPECT_EQ(ObDASFusionAdmissionResult::ADMITTED,
              decision.admission_result_);
    EXPECT_EQ(test_case.expected_dop_,
              decision.load_dop_cap_);
    EXPECT_EQ(test_case.expected_dop_,
              decision.target_path_dop_);
  }
}

// ============================================================================
// 第 2 节：ObSharedBitmapSlot 单测
// ============================================================================

TEST(ObSharedBitmapSlotTest, default_constructor)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_EQ(-1, slot.bitmap_occurrence_idx_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

TEST(ObSharedBitmapSlotTest, release_resets_state)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  slot.bitmap_occurrence_idx_ = 5;
  ATOMIC_STORE(&slot.is_built_, true);
  slot.build_ret_ = OB_ERR_UNEXPECTED;

  slot.release();

  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

// ============================================================================
// Section 4: ObDASFusionParallelCoordinator Tests
// ============================================================================

class ObDASFusionParallelCoordinatorTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObDASFusionParallelCoordinatorTest, init_with_invalid_count)
{
  ObDASFusionParallelCoordinator coord;
  EXPECT_EQ(OB_INVALID_ARGUMENT, coord.init(0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, coord.init(-1));
}

TEST_F(ObDASFusionParallelCoordinatorTest, init_success)
{
  ObDASFusionParallelCoordinator coord;
  EXPECT_EQ(OB_SUCCESS, coord.init(3));
  EXPECT_TRUE(coord.is_inited());
  EXPECT_EQ(OB_SUCCESS, coord.get_first_error());
}

TEST_F(ObDASFusionParallelCoordinatorTest, wait_not_inited)
{
  ObDASFusionParallelCoordinator coord;
  EXPECT_EQ(OB_NOT_INIT, coord.wait_all_complete(INT64_MAX));
  int64_t new_finished_cnt = -1;
  EXPECT_EQ(OB_NOT_INIT,
            coord.wait_for_progress(0, new_finished_cnt, INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, wait_for_progress_observes_finish)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(2));

  coord.on_work_unit_finish(OB_SUCCESS);
  int64_t new_finished_cnt = 0;
  EXPECT_EQ(OB_SUCCESS,
            coord.wait_for_progress(0, new_finished_cnt, INT64_MAX));
  EXPECT_EQ(1, new_finished_cnt);
}

TEST_F(ObDASFusionParallelCoordinatorTest, wait_for_progress_validation)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(2));

  int64_t new_finished_cnt = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            coord.wait_for_progress(-1, new_finished_cnt, INT64_MAX));
  const int64_t expired_ts = ObTimeUtility::current_time() - 1000;
  EXPECT_EQ(OB_TIMEOUT,
            coord.wait_for_progress(0, new_finished_cnt, expired_ts));
}

TEST_F(ObDASFusionParallelCoordinatorTest, single_child_finish)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  coord.on_work_unit_finish(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, multiple_children_finish)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.on_work_unit_finish(OB_SUCCESS);
  coord.on_work_unit_finish(OB_SUCCESS);
  coord.on_work_unit_finish(OB_SUCCESS);

  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, error_propagation)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.on_work_unit_finish(OB_SUCCESS);
  coord.on_work_unit_finish(OB_ERR_UNEXPECTED);
  coord.on_work_unit_finish(OB_SUCCESS);

  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, first_error_wins)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.set_first_error(OB_TIMEOUT);
  coord.on_work_unit_finish(OB_ERR_UNEXPECTED);
  coord.on_work_unit_finish(OB_SUCCESS);
  coord.on_work_unit_finish(OB_SUCCESS);

  EXPECT_EQ(OB_TIMEOUT, coord.get_first_error());
}

TEST_F(ObDASFusionParallelCoordinatorTest, set_first_error_ignores_success)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  coord.set_first_error(OB_SUCCESS);  // Should be ignored
  EXPECT_EQ(OB_SUCCESS, coord.get_first_error());

  coord.set_first_error(OB_ERR_UNEXPECTED);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());

  // Second error should NOT overwrite first
  coord.set_first_error(OB_TIMEOUT);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());
}

TEST_F(ObDASFusionParallelCoordinatorTest, timeout_when_not_all_finished)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.on_work_unit_finish(OB_SUCCESS);
  // Only 1 out of 3 finished

  int64_t expired_ts = ObTimeUtility::current_time() - 1000;
  EXPECT_EQ(OB_TIMEOUT, coord.wait_all_complete(expired_ts));
}

TEST_F(ObDASFusionParallelCoordinatorTest,
       ownership_barrier_waits_after_query_timeout)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(2));
  const int64_t expired_ts = ObTimeUtility::current_time() - 1000;
  ASSERT_EQ(OB_TIMEOUT, coord.wait_all_complete(expired_ts));
  coord.set_first_error(OB_TIMEOUT);

  std::thread worker([&coord]() {
    usleep(1000);
    coord.on_work_unit_finish(OB_CANCELED);
    usleep(1000);
    coord.on_work_unit_finish(OB_CANCELED);
  });
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete_barrier());
  EXPECT_EQ(2, coord.finished_cnt_);
  EXPECT_EQ(OB_TIMEOUT, coord.get_first_error());
  worker.join();
}

TEST_F(ObDASFusionParallelCoordinatorTest, reset)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.on_work_unit_finish(OB_ERR_UNEXPECTED);
  coord.on_work_unit_finish(OB_SUCCESS);
  coord.on_work_unit_finish(OB_SUCCESS);

  coord.reset();
  EXPECT_EQ(0, coord.total_cnt_);
  EXPECT_EQ(0, coord.finished_cnt_);
  EXPECT_EQ(OB_SUCCESS, coord.first_err_code_);
}

TEST_F(ObDASFusionParallelCoordinatorTest, concurrent_on_work_unit_finish)
{
  ObDASFusionParallelCoordinator coord;
  const int64_t N = 8;
  ASSERT_EQ(OB_SUCCESS, coord.init(N));

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&coord, i]() {
      usleep(static_cast<useconds_t>(i * 500));  // Stagger slightly
      coord.on_work_unit_finish(OB_SUCCESS);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 5000000;  // 5 seconds
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(timeout_ts));

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(ObDASFusionParallelCoordinatorTest, concurrent_with_one_error)
{
  ObDASFusionParallelCoordinator coord;
  const int64_t N = 8;
  ASSERT_EQ(OB_SUCCESS, coord.init(N));

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&coord, i]() {
      usleep(static_cast<useconds_t>(i * 500));
      int err = (i == 3) ? OB_CANCELED : OB_SUCCESS;
      coord.on_work_unit_finish(err);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 5000000;
  int ret = coord.wait_all_complete(timeout_ts);
  EXPECT_EQ(OB_CANCELED, ret);

  for (auto &t : threads) {
    t.join();
  }
}

// ============================================================================
// 第 5 节：ObDASFusionTaskExecEnv 单测
// ============================================================================

class ObDASFusionTaskExecEnvTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObDASFusionTaskExecEnvTest, default_constructor)
{
  ObDASFusionTaskExecEnv exec_env;
  EXPECT_EQ(-1, exec_env.path_idx_);
  EXPECT_EQ(nullptr, exec_env.source_child_iter_);
  EXPECT_EQ(nullptr, exec_env.child_iter_);
  EXPECT_EQ(nullptr, exec_env.child_exec_ctx_);
  EXPECT_EQ(nullptr, exec_env.child_eval_ctx_);
  EXPECT_EQ(nullptr, exec_env.child_search_ctx_);
  EXPECT_EQ(nullptr, exec_env.score_expr_);
  EXPECT_EQ(nullptr, exec_env.cloned_rtdef_root_);
  EXPECT_EQ(nullptr, exec_env.preser_exec_ctx_buf_);
  EXPECT_EQ(0, exec_env.preser_exec_ctx_size_);
}

TEST_F(ObDASFusionTaskExecEnvTest, release_clears_all)
{
  ObDASFusionTaskExecEnv exec_env;
  exec_env.release_resources();
  EXPECT_EQ(nullptr, exec_env.child_iter_);
  EXPECT_EQ(nullptr, exec_env.child_exec_ctx_);
  EXPECT_EQ(nullptr, exec_env.child_eval_ctx_);
  EXPECT_EQ(nullptr, exec_env.child_search_ctx_);
  EXPECT_EQ(nullptr, exec_env.cloned_rtdef_root_);
}

TEST_F(ObDASFusionTaskExecEnvTest, release_preserves_non_owning_source_iter)
{
  int source_iter_storage = 0;
  ObDASIter *source_iter =
      reinterpret_cast<ObDASIter *>(&source_iter_storage);
  ObDASFusionTaskExecEnv exec_env;
  exec_env.source_child_iter_ = source_iter;

  exec_env.release_resources();

  EXPECT_EQ(source_iter, exec_env.source_child_iter_);
  EXPECT_EQ(nullptr, exec_env.child_iter_);
}

// IVF children must retain the identity omitted by scan RtDef serialization.
TEST_F(ObDASFusionTaskExecEnvTest, deep_copy_ivf_children_preserves_namespace_identity)
{
  ObArenaAllocator alloc("TestLTClone");
  ObDASVecIndexIVFScanRtDef root;
  ObDASScanRtDef centroid;
  ObDASScanRtDef cid_vector;
  ObDASBaseRtDef *children[] = {&centroid, &cid_vector};
  root.children_ = children;
  root.children_cnt_ = ARRAYSIZEOF(children);
  ObDASFusionTaskExecEnv exec_env;
  for (uint64_t ns_id = 1001; ns_id <= 1008; ++ns_id) {
    for (int64_t i = 0; i < ARRAYSIZEOF(children); ++i) {
      ObDASScanRtDef *scan = static_cast<ObDASScanRtDef *>(children[i]);
      scan->lt_rt_.namespace_id_ = ns_id;
      scan->lt_rt_.logical_table_id_ = ns_id + 100;
      scan->lt_rt_.enabled_ = true;
    }
    ObDASBaseRtDef *cloned = nullptr;
    ASSERT_EQ(OB_SUCCESS, exec_env.deep_copy_rtdef_tree(alloc, &root, cloned));
    ASSERT_NE(nullptr, cloned);
    ASSERT_EQ(2, cloned->children_cnt_);
    centroid.lt_rt_.reset_runtime_identity();
    for (int64_t i = 0; i < cloned->children_cnt_; ++i) {
      const ObDASScanRtDef *scan = static_cast<const ObDASScanRtDef *>(cloned->children_[i]);
      EXPECT_EQ(ns_id, scan->lt_rt_.namespace_id_);
      EXPECT_EQ(ns_id + 100, scan->lt_rt_.logical_table_id_);
      EXPECT_TRUE(scan->lt_rt_.enabled_);
      EXPECT_EQ(nullptr, scan->lt_scan_rt_);
    }
    exec_env.destroy_rtdef_tree(cloned);
  }
}

TEST_F(ObDASFusionTaskExecEnvTest, deep_copy_rtdef_tree_preserves_search_cost_only)
{
  ObArenaAllocator alloc("TestRTClone");
  ObDASScalarCtDef root_ctdef(alloc);
  ObDASScalarCtDef valid_child_ctdef(alloc);
  ObDASScalarCtDef invalid_child_ctdef(alloc);
  ObDASScalarRtDef root_rtdef;
  ObDASScalarRtDef valid_child_rtdef;
  ObDASScalarRtDef invalid_child_rtdef;
  ObDASBaseRtDef *children[] = {&valid_child_rtdef, &invalid_child_rtdef};

  root_rtdef.ctdef_ = &root_ctdef;
  root_rtdef.children_ = children;
  root_rtdef.children_cnt_ = ARRAYSIZEOF(children);
  root_rtdef.cost_ = ObDASSearchCost(100);
  root_rtdef.set_allow_probe(true);
  valid_child_rtdef.ctdef_ = &valid_child_ctdef;
  valid_child_rtdef.cost_ = ObDASSearchCost(90000);
  valid_child_rtdef.set_allow_probe(true);
  invalid_child_rtdef.ctdef_ = &invalid_child_ctdef;
  invalid_child_rtdef.cost_.reset();
  invalid_child_rtdef.set_allow_probe(true);

  ObDASFusionTaskExecEnv exec_env;
  ObDASBaseRtDef *cloned_base_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            exec_env.deep_copy_rtdef_tree(alloc, &root_rtdef, cloned_base_rtdef));
  ASSERT_NE(nullptr, cloned_base_rtdef);
  ASSERT_EQ(2, cloned_base_rtdef->children_cnt_);
  ObDASScalarRtDef *cloned_root_rtdef =
      static_cast<ObDASScalarRtDef *>(cloned_base_rtdef);
  ObDASScalarRtDef *cloned_valid_child_rtdef =
      static_cast<ObDASScalarRtDef *>(cloned_base_rtdef->children_[0]);
  ObDASScalarRtDef *cloned_invalid_child_rtdef =
      static_cast<ObDASScalarRtDef *>(cloned_base_rtdef->children_[1]);

  EXPECT_TRUE(cloned_root_rtdef->cost_.is_valid());
  EXPECT_EQ(100, cloned_root_rtdef->cost_.cost());
  EXPECT_FALSE(cloned_root_rtdef->get_allow_probe());
  EXPECT_TRUE(cloned_valid_child_rtdef->cost_.is_valid());
  EXPECT_EQ(90000, cloned_valid_child_rtdef->cost_.cost());
  EXPECT_FALSE(cloned_valid_child_rtdef->get_allow_probe());
  EXPECT_FALSE(cloned_invalid_child_rtdef->cost_.is_valid());
  EXPECT_FALSE(cloned_invalid_child_rtdef->get_allow_probe());

  exec_env.destroy_rtdef_tree(cloned_base_rtdef);
}

TEST_F(ObDASFusionTaskExecEnvTest, deep_copy_rtdef_tree_preserves_match_runtime_state)
{
  ObArenaAllocator alloc("TestMatchClone");
  ObDASMatchCtDef match_ctdef(alloc);
  ObDASIRScanCtDef ir_ctdef(alloc);
  ObDASMatchRtDef match_rtdef;
  ObDASBaseCtDef *ctdef_children[] = {&ir_ctdef};

  ir_ctdef.is_async_index_ = true;
  match_ctdef.children_ = ctdef_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.cost_ = ObDASSearchCost(80);
  match_rtdef.query_tokens_prepared_ = true;
  ASSERT_EQ(OB_SUCCESS, match_rtdef.query_tokens_.push_back(ObString::make_string("omega")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef.query_tokens_.push_back(ObString::make_string("gamma")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef.token_costs_.push_back(ObDASSearchCost(100)));
  ASSERT_EQ(OB_SUCCESS, match_rtdef.token_costs_.push_back(ObDASSearchCost(50)));

  ObDASFusionTaskExecEnv exec_env;
  ObDASBaseRtDef *cloned_base_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            exec_env.deep_copy_rtdef_tree(alloc, &match_rtdef, cloned_base_rtdef));
  ASSERT_NE(nullptr, cloned_base_rtdef);
  ObDASMatchRtDef *cloned_match_rtdef =
      static_cast<ObDASMatchRtDef *>(cloned_base_rtdef);

  EXPECT_TRUE(cloned_match_rtdef->cost_.is_valid());
  EXPECT_EQ(80, cloned_match_rtdef->cost_.cost());
  EXPECT_TRUE(cloned_match_rtdef->query_tokens_prepared_);
  ASSERT_EQ(2, cloned_match_rtdef->query_tokens_.count());
  EXPECT_EQ(ObString::make_string("omega"), cloned_match_rtdef->query_tokens_.at(0));
  EXPECT_EQ(ObString::make_string("gamma"), cloned_match_rtdef->query_tokens_.at(1));
  EXPECT_NE(match_rtdef.query_tokens_.at(0).ptr(), cloned_match_rtdef->query_tokens_.at(0).ptr());
  EXPECT_NE(match_rtdef.query_tokens_.at(1).ptr(), cloned_match_rtdef->query_tokens_.at(1).ptr());
  ASSERT_EQ(2, cloned_match_rtdef->token_costs_.count());
  EXPECT_EQ(100, cloned_match_rtdef->token_costs_.at(0).cost());
  EXPECT_EQ(50, cloned_match_rtdef->token_costs_.at(1).cost());
  EXPECT_EQ(OB_SUCCESS, cloned_match_rtdef->align_token_costs());

  exec_env.destroy_rtdef_tree(cloned_base_rtdef);
}

TEST_F(ObDASFusionTaskExecEnvTest, deep_copy_rtdef_tree_preserves_match_runtime_state_without_token_costs)
{
  ObArenaAllocator alloc("MatchNoCost");
  ObDASMatchCtDef match_ctdef(alloc);
  ObDASIRScanCtDef ir_ctdef(alloc);
  ObDASMatchRtDef match_rtdef;
  ObDASBaseCtDef *ctdef_children[] = {&ir_ctdef};

  ir_ctdef.is_async_index_ = true;
  match_ctdef.children_ = ctdef_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.cost_ = ObDASSearchCost(80);
  match_rtdef.query_tokens_prepared_ = true;
  ASSERT_EQ(OB_SUCCESS, match_rtdef.query_tokens_.push_back(ObString::make_string("omega")));

  ObDASFusionTaskExecEnv exec_env;
  ObDASBaseRtDef *cloned_base_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            exec_env.deep_copy_rtdef_tree(alloc, &match_rtdef, cloned_base_rtdef));
  ASSERT_NE(nullptr, cloned_base_rtdef);
  ObDASMatchRtDef *cloned_match_rtdef =
      static_cast<ObDASMatchRtDef *>(cloned_base_rtdef);
  EXPECT_TRUE(cloned_match_rtdef->cost_.is_valid());
  EXPECT_EQ(80, cloned_match_rtdef->cost_.cost());
  EXPECT_TRUE(cloned_match_rtdef->query_tokens_prepared_);
  ASSERT_EQ(1, cloned_match_rtdef->query_tokens_.count());
  EXPECT_EQ(ObString::make_string("omega"), cloned_match_rtdef->query_tokens_.at(0));
  EXPECT_NE(match_rtdef.query_tokens_.at(0).ptr(), cloned_match_rtdef->query_tokens_.at(0).ptr());
  EXPECT_TRUE(cloned_match_rtdef->token_costs_.empty());
  EXPECT_EQ(OB_ERR_UNEXPECTED, cloned_match_rtdef->align_token_costs());

  exec_env.destroy_rtdef_tree(cloned_base_rtdef);
}

TEST_F(ObDASFusionTaskExecEnvTest, prepare_resources_null_sources)
{
  ObDASFusionTaskExecEnv exec_env;
  // src pointers are null by default
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            exec_env.prepare_resources());
}

TEST_F(ObDASFusionTaskExecEnvTest, init_with_invalid_path_idx)
{
  ObArenaAllocator alloc("TestRT");
  ObDASFusionCtDef ctdef(alloc);
  ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef.children_cnt_ = 2;

  // score_exprs_ has 3 elements (2 paths + fusion), but path_idx = 5 is out of range
  ObExpr dummy_expr1, dummy_expr2, dummy_expr3;
  ctdef.score_exprs_.init(3);
  ctdef.score_exprs_.push_back(&dummy_expr1);
  ctdef.score_exprs_.push_back(&dummy_expr2);
  ctdef.score_exprs_.push_back(&dummy_expr3);

  // rowid_exprs_ has 1 element
  ObExpr rowid_expr;
  ctdef.rowid_exprs_.init(1);
  ctdef.rowid_exprs_.push_back(&rowid_expr);

  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  eval_ctx.max_batch_size_ = 256;

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);

  ObDASFusionLogicalPath logical_path;
  logical_path.path_id_ = 5;
  ObDASFusionTaskExecEnv exec_env;
  // path_idx = 5, but score_exprs_ only has count=3, valid range is [0, 1]
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            exec_env.init(
                logical_path, ctdef,
                exec_ctx, eval_ctx, search_ctx,
                nullptr, 0));
}

TEST_F(ObDASFusionTaskExecEnvTest, init_does_not_require_rowid_expr)
{
  ObArenaAllocator alloc("TestRT");
  ObDASFusionCtDef ctdef(alloc);
  ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef.children_cnt_ = 1;

  // score_exprs_ has elements but rowid_exprs_ is empty
  ObExpr dummy_expr1, dummy_expr2;
  ctdef.score_exprs_.init(2);
  ctdef.score_exprs_.push_back(&dummy_expr1);
  ctdef.score_exprs_.push_back(&dummy_expr2);
  // rowid_exprs_ is empty

  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);

  ObDASFusionLogicalPath logical_path;
  logical_path.path_id_ = 0;
  ObDASFusionTaskExecEnv exec_env;
  EXPECT_EQ(OB_SUCCESS,
            exec_env.init(
                logical_path, ctdef,
                exec_ctx, eval_ctx, search_ctx,
                nullptr, 0));
}

TEST_F(ObDASFusionTaskExecEnvTest, init_success)
{
  ObArenaAllocator alloc("TestRT");
  ObDASFusionCtDef ctdef(alloc);
  ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef.children_cnt_ = 2;

  ObExpr rowid_expr, score_expr0, score_expr1, fusion_score;
  ctdef.rowid_exprs_.init(1);
  ctdef.rowid_exprs_.push_back(&rowid_expr);

  ctdef.score_exprs_.init(3);
  ctdef.score_exprs_.push_back(&score_expr0);
  ctdef.score_exprs_.push_back(&score_expr1);
  ctdef.score_exprs_.push_back(&fusion_score);

  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  eval_ctx.max_batch_size_ = 256;

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);

  ObDASFusionLogicalPath logical_path;
  logical_path.path_id_ = 0;
  ObDASFusionTaskExecEnv exec_env;
  ASSERT_EQ(OB_SUCCESS,
            exec_env.init(
                logical_path, ctdef,
                exec_ctx, eval_ctx, search_ctx,
                nullptr, 0));

  EXPECT_EQ(0, exec_env.path_idx_);
  EXPECT_EQ(&score_expr0, exec_env.score_expr_);
  EXPECT_EQ(&exec_ctx, exec_env.src_exec_ctx_);
  EXPECT_EQ(&eval_ctx, exec_env.src_eval_ctx_);
  EXPECT_EQ(&search_ctx, exec_env.src_search_ctx_);
  EXPECT_EQ(&ctdef, exec_env.fusion_ctdef_);
}

// ============================================================================
// 第 6 节：ObDASFusionParallelCtx 规划和工作量估算单测
// ============================================================================

class ObDASFusionParallelCtxTest : public ::testing::Test
{
protected:
  ObDASFusionParallelCtxTest()
      : alloc_("TestPCtx"),
        ctdef_(alloc_)
  {}

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
    ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
  }

  ObArenaAllocator alloc_;
  ObDASFusionCtDef ctdef_;
};

TEST_F(ObDASFusionParallelCtxTest, parallel_planning_checks_only_hard_eligibility)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.is_top_k_query_ = true;
  ctdef_.children_cnt_ = 1;
  ctdef_.query_dop_ = ObDASFusionDopPolicy::AUTO_QUERY_DOP;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);

  EXPECT_TRUE(ObDASFusionParallelCtx::is_parallel_execution_eligible(
      ctdef_, search_ctx));

  ctdef_.enable_parallel_ = false;
  EXPECT_FALSE(ObDASFusionParallelCtx::is_parallel_execution_eligible(
      ctdef_, search_ctx));
  ctdef_.enable_parallel_ = true;

  ctdef_.is_top_k_query_ = false;
  EXPECT_FALSE(ObDASFusionParallelCtx::is_parallel_execution_eligible(
      ctdef_, search_ctx));
  ctdef_.is_top_k_query_ = true;

  ctdef_.children_cnt_ = 0;
  EXPECT_FALSE(ObDASFusionParallelCtx::is_parallel_execution_eligible(
      ctdef_, search_ctx));
  ctdef_.children_cnt_ = 1;

  search_ctx.set_rowid_type(DAS_ROWID_TYPE_COMPACT);
  EXPECT_FALSE(ObDASFusionParallelCtx::is_parallel_execution_eligible(
      ctdef_, search_ctx));
}

TEST_F(ObDASFusionParallelCtxTest,
       auto_cpu_sample_failure_forces_whole_fusion_serial)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.is_top_k_query_ = true;
  ctdef_.children_cnt_ = 2;
  ctdef_.query_dop_ = ObDASFusionDopPolicy::AUTO_QUERY_DOP;
  ctdef_.has_search_subquery_ = true;
  ctdef_.has_vector_subquery_ = true;
  ctdef_.set_search_index(0);

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  search_ctx.set_rowid_type(DAS_ROWID_TYPE_UINT64);
  ObDASFusionRtDef fusion_rtdef;
  ObDASIter *children[2] = {nullptr, nullptr};
  ObSEArray<int64_t, 2> path_top_k_limits;
  ASSERT_EQ(OB_SUCCESS, path_top_k_limits.push_back(10));
  ASSERT_EQ(OB_SUCCESS, path_top_k_limits.push_back(10));

  ObDASFusionParallelCtx parallel_ctx;
  ASSERT_EQ(OB_SUCCESS,
            parallel_ctx.plan_paths(
                alloc_, ctdef_, fusion_rtdef, search_ctx,
                children, ARRAYSIZEOF(children), path_top_k_limits));
  EXPECT_DOUBLE_EQ(-1.0,
                   parallel_ctx.get_tenant_cpu_pressure_percent());
  EXPECT_TRUE(parallel_ctx.should_fallback_to_serial());
  EXPECT_EQ(0, parallel_ctx.get_path_count());
}

TEST_F(ObDASFusionParallelCtxTest,
       single_and_async_paths_are_deferred_to_path_planning)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.is_top_k_query_ = true;
  ctdef_.children_cnt_ = 1;
  ctdef_.query_dop_ = ObDASFusionDopPolicy::AUTO_QUERY_DOP;
  ctdef_.has_search_subquery_ = true;
  ctdef_.set_search_index(0);
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  ObDASAsyncIndexQueryCtx async_query_ctx;
  search_ctx.set_async_query_ctx(&async_query_ctx);

  EXPECT_TRUE(ObDASFusionParallelCtx::is_parallel_execution_eligible(
      ctdef_, search_ctx));
}

TEST_F(ObDASFusionParallelCtxTest,
       query_path_uses_effective_search_driver_top_k_limit)
{
  ctdef_.is_top_k_query_ = true;
  ctdef_.children_cnt_ = 1;
  ctdef_.query_dop_ = 1;
  ctdef_.has_search_subquery_ = true;
  ctdef_.set_search_index(0);

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  search_ctx.set_rowid_type(DAS_ROWID_TYPE_UINT64);

  ObDASFusionRtDef fusion_rtdef;
  ObDASFusionRtDef child_rtdef;
  ObDASBaseRtDef *child_rtdefs[] = {&child_rtdef};
  fusion_rtdef.children_ = child_rtdefs;
  fusion_rtdef.children_cnt_ = ARRAYSIZEOF(child_rtdefs);

  ObDASSearchDriverIter search_iter;
  search_iter.top_k_limit_param_.limit_ = -1;
  ObDASIter *children[] = {&search_iter};
  ObSEArray<int64_t, 1> expr_path_top_k_limits;
  ASSERT_EQ(OB_SUCCESS, expr_path_top_k_limits.push_back(10));

  ObDASFusionParallelCtx parallel_ctx;
  ASSERT_EQ(OB_SUCCESS,
            parallel_ctx.build_path_adapters(
                alloc_, ctdef_, fusion_rtdef, search_ctx,
                children, ARRAYSIZEOF(children),
                expr_path_top_k_limits));
  ASSERT_EQ(1, parallel_ctx.get_path_count());
  const ObDASFusionPathAdapter *path_adapter =
      parallel_ctx.get_path_adapter(0);
  ASSERT_NE(nullptr, path_adapter);
  EXPECT_EQ(-1,
            path_adapter->get_logical_path().path_top_k_limit_);
  ASSERT_EQ(1, path_adapter->get_slice_count());
  ASSERT_NE(nullptr, path_adapter->get_slice(0));
  EXPECT_EQ(-1, path_adapter->get_slice(0)->path_top_k_limit_);
}

TEST_F(ObDASFusionParallelCtxTest,
       parallel_work_estimate_sums_only_range_divisible_ror_ops)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  TestEstimatedWorkSearchOp root(search_ctx, -1);
  TestEstimatedWorkSearchOp token(search_ctx, 120);
  TestEstimatedWorkSearchOp scalar_ror(search_ctx, 80);
  TestEstimatedWorkSearchOp scalar_scan(search_ctx, 800000);
  TestEstimatedWorkSearchOp async_scalar_ror(search_ctx, 800000);
  TestEstimatedWorkSearchOp bitmap(search_ctx, -1);
  TestEstimatedWorkSearchOp bitmap_token(search_ctx, 500000);
  TestEstimatedWorkSearchOp primary_probe(search_ctx, 1000000);
  token.set_work_role(DAS_SEARCH_OP_TOKEN, true);
  scalar_ror.set_work_role(DAS_SEARCH_OP_SCALAR_INDEX_ROR, true);
  scalar_scan.set_work_role(DAS_SEARCH_OP_SCALAR_SCAN, false);
  async_scalar_ror.set_work_role(DAS_SEARCH_OP_ASYNC_SCALAR_ROR, true);
  bitmap.set_work_role(DAS_SEARCH_OP_BITMAP, false);
  bitmap_token.set_work_role(DAS_SEARCH_OP_TOKEN, true);
  primary_probe.set_work_role(
      DAS_SEARCH_OP_SCALAR_PRIMARY_ROR, false);
  primary_probe.set_probe_mode(true);
  ObIDASSearchOp *bitmap_children[] = {&bitmap_token};
  bitmap.set_children(bitmap_children, ARRAYSIZEOF(bitmap_children));
  ObIDASSearchOp *children[] = {
      &token, &scalar_ror, &scalar_scan, &async_scalar_ror,
      &bitmap, &primary_probe};
  root.set_children(children, ARRAYSIZEOF(children));

  ASSERT_EQ(OB_SUCCESS, root.prepare_parallel_work_estimate());
  const ObDASSearchWorkEstimate estimate =
      root.get_parallel_work_estimate();
  EXPECT_EQ(2, estimate.ror_op_count_);
  EXPECT_EQ(200, estimate.known_ror_work_row_count_);
  EXPECT_EQ(0, estimate.unknown_ror_op_count_);
  EXPECT_EQ(200, estimate.get_ror_work_row_count());
  EXPECT_EQ(0, root.get_prepare_count());
  EXPECT_EQ(1, token.get_prepare_count());
  EXPECT_EQ(1, scalar_ror.get_prepare_count());
  EXPECT_EQ(0, scalar_scan.get_prepare_count());
  EXPECT_EQ(0, async_scalar_ror.get_prepare_count());
  EXPECT_EQ(0, bitmap.get_prepare_count());
  EXPECT_EQ(0, bitmap_token.get_prepare_count());
  EXPECT_EQ(0, primary_probe.get_prepare_count());
}

TEST_F(ObDASFusionParallelCtxTest,
       parallel_work_estimate_rejects_incomplete_ror_sum)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  TestEstimatedWorkSearchOp root(search_ctx, -1);
  TestEstimatedWorkSearchOp known_token(search_ctx, 120);
  TestEstimatedWorkSearchOp unknown_token(search_ctx, -1);
  known_token.set_work_role(DAS_SEARCH_OP_TOKEN, true);
  unknown_token.set_work_role(DAS_SEARCH_OP_TOKEN, true);
  ObIDASSearchOp *children[] = {&known_token, &unknown_token};
  root.set_children(children, ARRAYSIZEOF(children));

  const ObDASSearchWorkEstimate estimate =
      root.get_parallel_work_estimate();
  EXPECT_EQ(2, estimate.ror_op_count_);
  EXPECT_EQ(120, estimate.known_ror_work_row_count_);
  EXPECT_EQ(1, estimate.unknown_ror_op_count_);
  EXPECT_EQ(-1, estimate.get_ror_work_row_count());
}

TEST_F(ObDASFusionParallelCtxTest,
       async_segment_work_estimate_counts_logical_tree_once)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  TestEstimatedWorkSearchOp segment_merge(search_ctx, -1);
  TestEstimatedWorkSearchOp first_segment_root(search_ctx, -1);
  TestEstimatedWorkSearchOp second_segment_root(search_ctx, -1);
  TestEstimatedWorkSearchOp first_async_token(search_ctx, 50000);
  TestEstimatedWorkSearchOp second_async_token(search_ctx, 50000);
  segment_merge.set_work_role(
      DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, false);
  first_async_token.set_work_role(DAS_SEARCH_OP_ASYNC_TOKEN, true);
  second_async_token.set_work_role(DAS_SEARCH_OP_ASYNC_TOKEN, true);
  ObIDASSearchOp *first_segment_children[] = {&first_async_token};
  ObIDASSearchOp *second_segment_children[] = {&second_async_token};
  first_segment_root.set_children(
      first_segment_children, ARRAYSIZEOF(first_segment_children));
  second_segment_root.set_children(
      second_segment_children, ARRAYSIZEOF(second_segment_children));
  ObIDASSearchOp *segment_children[] = {
      &first_segment_root, &second_segment_root};
  segment_merge.set_children(
      segment_children, ARRAYSIZEOF(segment_children));

  ASSERT_EQ(OB_SUCCESS,
            segment_merge.prepare_parallel_work_estimate());
  const ObDASSearchWorkEstimate estimate =
      segment_merge.get_parallel_work_estimate();
  EXPECT_EQ(1, estimate.ror_op_count_);
  EXPECT_EQ(50000, estimate.known_ror_work_row_count_);
  EXPECT_EQ(0, estimate.unknown_ror_op_count_);
  EXPECT_EQ(50000, estimate.get_ror_work_row_count());
  EXPECT_EQ(1, first_async_token.get_prepare_count());
  EXPECT_EQ(0, second_async_token.get_prepare_count());
}

TEST_F(ObDASFusionParallelCtxTest,
       scalar_scan_init_rejects_missing_rowid_exprs)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  ObDASScalarScanCtDef scalar_ctdef(alloc_);
  ObDASScalarScanRtDef scalar_rtdef;
  ObExpr producer_expr;
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.push_back(&producer_expr));

  ObDASScalarScanOp scan_op(search_ctx);
  ObDASScalarScanOpParam scan_param(&scalar_ctdef, &scalar_rtdef);
  EXPECT_EQ(OB_ERR_UNEXPECTED, scan_op.init(scan_param));
}

TEST_F(ObDASFusionParallelCtxTest,
       scalar_admission_counts_only_ror_operator)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  ObDASScalarScanCtDef scalar_ctdef(alloc_);
  ObDASScalarScanRtDef scalar_rtdef;
  ObExpr rowid_expr;
  ExprFixedArray rowid_exprs(alloc_);
  ASSERT_EQ(OB_SUCCESS, rowid_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, rowid_exprs.push_back(&rowid_expr));
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.push_back(&rowid_expr));
  search_ctx.rowid_exprs_ = &rowid_exprs;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
  search_ctx.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs;
  search_ctx.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
  scalar_rtdef.set_estimated_work_row_count(321);

  ObDASScalarScanOp scan_op(search_ctx);
  ObDASScalarScanOpParam scan_param(&scalar_ctdef, &scalar_rtdef);
  ASSERT_EQ(OB_SUCCESS, scan_op.init(scan_param));
  ObDASSearchWorkEstimate estimate =
      scan_op.get_parallel_work_estimate();
  EXPECT_EQ(0, estimate.ror_op_count_);
  EXPECT_EQ(0, estimate.get_ror_work_row_count());

  ObDASScalarIndexROROp ror_op(search_ctx);
  ObDASScalarIndexROROpParam ror_param(
      &scalar_ctdef, &scalar_rtdef);
  ASSERT_EQ(OB_SUCCESS, ror_op.init(ror_param));
  estimate = ror_op.get_parallel_work_estimate();
  EXPECT_EQ(1, estimate.ror_op_count_);
  EXPECT_EQ(321, estimate.get_ror_work_row_count());
}

// A task owns one rtdef, but each Segment Group generates another Scalar ROR.
// Both the original query bounds and the other Group's slice must stay intact.
TEST_F(ObDASFusionParallelCtxTest, scalar_ror_ranges_are_owned_per_group)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  ObDASScalarScanCtDef ctdef(alloc_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ExprFixedArray rowid_exprs(alloc_);
  ASSERT_EQ(OB_SUCCESS, rowid_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, rowid_exprs.push_back(&rowid_expr));
  ASSERT_EQ(OB_SUCCESS, ctdef.rowkey_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, ctdef.rowkey_exprs_.push_back(&rowid_expr));
  search_ctx.rowid_exprs_ = &rowid_exprs;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
  search_ctx.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs;
  search_ctx.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;

  ObObj starts[2];
  ObObj ends[2];
  starts[0].set_min_value();
  ends[0].set_max_value();
  starts[1].set_uint64(228);
  ends[1].set_uint64(1024);
  for (int64_t i = 0; i < 2; ++i) {
    ObNewRange range;
    range.table_id_ = 12345;
    range.start_key_.assign(&starts[i], 1);
    range.end_key_.assign(&ends[i], 1);
    ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(range));
  }
  ObDASScalarIndexROROp first(search_ctx);
  ObDASScalarIndexROROp second(search_ctx);
  ObDASScalarIndexROROp *groups[] = {&first, &second};
  for (auto *group : groups) {
    group->scalar_ctdef_ = &ctdef;
    group->scalar_rtdef_ = &rtdef;
    ASSERT_EQ(OB_SUCCESS, group->rowid_output_exprs_.init(rowid_exprs, 1));
  }

  // Reprepare with a wider slice and then a narrower slice, as on rescan.
  const uint64_t lows[] = {228, 100, 300};
  const uint64_t highs[] = {1024, 2000, 900};
  for (int64_t round = 0; round < 3; ++round) {
    ObObj lo;
    ObObj hi;
    lo.set_uint64(lows[round]);
    hi.set_uint64(highs[round]);
    search_ctx.set_docid_range(lo, hi);
    for (auto *group : groups) {
      ASSERT_EQ(OB_SUCCESS, group->prepare_scan_ranges(&rtdef));
      ASSERT_EQ(2, group->scan_param_.key_ranges_.count());
      ASSERT_EQ(OB_SUCCESS, group->narrow_scan_ranges_by_docid_range());
      const ObNewRange &whole = group->scan_param_.key_ranges_.at(0);
      EXPECT_EQ(lows[round], whole.start_key_.get_obj_ptr()[0].get_uint64());
      EXPECT_EQ(highs[round], whole.end_key_.get_obj_ptr()[0].get_uint64());
      EXPECT_TRUE(whole.border_flag_.inclusive_start());
      EXPECT_TRUE(whole.border_flag_.inclusive_end());
      const ObNewRange &bounded = group->scan_param_.key_ranges_.at(1);
      EXPECT_EQ(MAX(lows[round], 228UL), bounded.start_key_.get_obj_ptr()[0].get_uint64());
      EXPECT_EQ(MIN(highs[round], 1024UL), bounded.end_key_.get_obj_ptr()[0].get_uint64());
      EXPECT_EQ(round == 2, bounded.border_flag_.inclusive_start());
      EXPECT_EQ(round == 2, bounded.border_flag_.inclusive_end());
      // The finite query bounds are exclusive until the task really tightens them.
      EXPECT_TRUE(starts[0].is_min_value());
      EXPECT_TRUE(ends[0].is_max_value());
      EXPECT_EQ(228UL, starts[1].get_uint64());
      EXPECT_EQ(1024UL, ends[1].get_uint64());
      EXPECT_FALSE(rtdef.key_ranges_.at(0).border_flag_.inclusive_start());
      EXPECT_FALSE(rtdef.key_ranges_.at(0).border_flag_.inclusive_end());
    }
    EXPECT_NE(first.scan_param_.key_ranges_.at(0).start_key_.get_obj_ptr(),
              second.scan_param_.key_ranges_.at(0).start_key_.get_obj_ptr());
  }
  ASSERT_EQ(OB_SUCCESS, first.do_close());
  // Closing another Group cannot invalidate this Group's owned range.
  EXPECT_EQ(300UL, second.scan_param_.key_ranges_.at(0).start_key_.get_obj_ptr()[0].get_uint64());
  ASSERT_EQ(OB_SUCCESS, second.do_close());
}

TEST_F(ObDASFusionParallelCtxTest,
       dummy_op_does_not_enter_ror_admission_work)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  ObDASDummyOp dummy_op(search_ctx);
  ObDASDummyOpParam dummy_param;
  ASSERT_EQ(OB_SUCCESS, dummy_op.init(dummy_param));

  const ObDASSearchWorkEstimate estimate =
      dummy_op.get_parallel_work_estimate();
  EXPECT_EQ(0, estimate.ror_op_count_);
  EXPECT_EQ(0, estimate.get_ror_work_row_count());
}
TEST_F(ObDASFusionParallelCtxTest, build_work_units_preserves_work_order_and_type)
{
  ObDASFusionParallelCtx ctx;
  ObDASFusionPathSliceWork whole_path_work;
  ObDASFusionPathSliceWork range_work;
  whole_path_work.exec_env_.path_idx_ = 0;
  range_work.exec_env_.path_idx_ = 1;
  ASSERT_EQ(OB_SUCCESS,
            whole_path_work.work_slice_.init_whole_path(0));
  init_docid_range_slice(range_work, 1, 0, 10);
  ASSERT_EQ(OB_SUCCESS, ctx.works_.push_back(&whole_path_work));
  ASSERT_EQ(OB_SUCCESS, ctx.works_.push_back(&range_work));

  ASSERT_EQ(OB_SUCCESS, ctx.build_work_units());
  ASSERT_EQ(2, ctx.get_work_unit_count());
  ASSERT_NE(nullptr, ctx.get_work_unit(0));
  ASSERT_NE(nullptr, ctx.get_work_unit(1));
  EXPECT_EQ(0, ctx.get_work_unit(0)->unit_id_);
  EXPECT_EQ(0, ctx.get_work_unit(0)->path_id_);
  EXPECT_EQ(-1, ctx.get_work_unit(0)->dependency_unit_id_);
  EXPECT_EQ(ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE,
            ctx.get_work_unit(0)->kind_);
  ASSERT_NE(nullptr, ctx.get_work_unit(0)->work_);
  EXPECT_EQ(0, ctx.get_work_unit(0)->work_->get_path_id());
  EXPECT_EQ(1, ctx.get_work_unit(1)->unit_id_);
  EXPECT_EQ(1, ctx.get_work_unit(1)->path_id_);
  EXPECT_EQ(-1, ctx.get_work_unit(1)->dependency_unit_id_);
  EXPECT_EQ(ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE,
            ctx.get_work_unit(1)->kind_);
  ASSERT_NE(nullptr, ctx.get_work_unit(1)->work_);
  EXPECT_EQ(1, ctx.get_work_unit(1)->work_->get_path_id());
}

TEST_F(ObDASFusionParallelCtxTest, build_work_units_adds_bitmap_prepare_dependency)
{
  ObDASFusionParallelCtx ctx;
  ObDASFusionBitmapPrepareWork prepare_work;
  ObDASFusionPathSliceWork whole_path_work;
  ObDASFusionPathSliceWork range_works[2];
  prepare_work.exec_env_.path_idx_ = 1;
  whole_path_work.exec_env_.path_idx_ = 0;
  range_works[0].exec_env_.path_idx_ = 1;
  range_works[1].exec_env_.path_idx_ = 1;
  range_works[0].needs_shared_bitmaps_ = true;
  range_works[1].needs_shared_bitmaps_ = true;
  ASSERT_EQ(OB_SUCCESS,
            whole_path_work.work_slice_.init_whole_path(0));
  init_docid_range_slice(range_works[0], 1, 0, 10);
  init_docid_range_slice(range_works[1], 1, 1, 10);
  ctx.bitmap_prepare_work_ = &prepare_work;
  ASSERT_EQ(OB_SUCCESS, ctx.works_.push_back(&prepare_work));
  ASSERT_EQ(OB_SUCCESS, ctx.works_.push_back(&whole_path_work));
  ASSERT_EQ(OB_SUCCESS, ctx.works_.push_back(&range_works[0]));
  ASSERT_EQ(OB_SUCCESS, ctx.works_.push_back(&range_works[1]));

  ASSERT_EQ(OB_SUCCESS, ctx.build_work_units());
  ASSERT_EQ(4, ctx.get_work_unit_count());
  EXPECT_EQ(ObDASFusionWorkUnitKind::PREPARE,
            ctx.get_work_unit(0)->kind_);
  ASSERT_NE(nullptr, ctx.get_work_unit(0)->work_);
  EXPECT_EQ(1, ctx.get_work_unit(0)->work_->get_path_id());
  EXPECT_EQ(-1, ctx.get_work_unit(0)->dependency_unit_id_);
  EXPECT_EQ(-1, ctx.get_work_unit(1)->dependency_unit_id_);
  EXPECT_EQ(0, ctx.get_work_unit(2)->dependency_unit_id_);
  EXPECT_EQ(0, ctx.get_work_unit(3)->dependency_unit_id_);
}

TEST_F(ObDASFusionParallelCtxTest, build_work_units_rejects_second_bitmap_prepare)
{
  ObDASFusionParallelCtx ctx;
  ObDASFusionBitmapPrepareWork prepare_works[2];
  prepare_works[0].exec_env_.path_idx_ = 0;
  prepare_works[1].exec_env_.path_idx_ = 0;
  ctx.bitmap_prepare_work_ = &prepare_works[0];
  ASSERT_EQ(OB_SUCCESS,
            ctx.works_.push_back(&prepare_works[0]));
  ASSERT_EQ(OB_SUCCESS,
            ctx.works_.push_back(&prepare_works[1]));

  EXPECT_EQ(OB_ERR_UNEXPECTED, ctx.build_work_units());
  ASSERT_EQ(1, ctx.get_work_unit_count());
  EXPECT_EQ(ObDASFusionWorkUnitKind::PREPARE,
            ctx.get_work_unit(0)->kind_);
}

TEST_F(ObDASFusionParallelCtxTest, work_unit_ready_follows_dependency_state)
{
  ObDASFusionParallelCtx ctx;
  ObDASFusionBitmapPrepareWork prepare_work;
  ObDASFusionPathSliceWork range_work;
  prepare_work.exec_env_.path_idx_ = 0;
  range_work.exec_env_.path_idx_ = 0;
  range_work.needs_shared_bitmaps_ = true;
  init_docid_range_slice(range_work, 0, 0, 10);
  ctx.bitmap_prepare_work_ = &prepare_work;
  ASSERT_EQ(OB_SUCCESS, ctx.works_.push_back(&prepare_work));
  ASSERT_EQ(OB_SUCCESS, ctx.works_.push_back(&range_work));
  ASSERT_EQ(OB_SUCCESS, ctx.build_work_units());

  bool is_ready = false;
  ASSERT_EQ(OB_SUCCESS,
            ctx.check_work_unit_ready(*ctx.get_work_unit(0), is_ready));
  EXPECT_TRUE(is_ready);
  ASSERT_EQ(OB_SUCCESS,
            ctx.check_work_unit_ready(*ctx.get_work_unit(1), is_ready));
  EXPECT_FALSE(is_ready);

  ATOMIC_STORE(&ctx.get_work_unit(0)->finished_, true);
  ATOMIC_STORE(&ctx.get_work_unit(0)->err_code_, OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS,
            ctx.check_work_unit_ready(*ctx.get_work_unit(1), is_ready));
  EXPECT_TRUE(is_ready);

  ATOMIC_STORE(&ctx.get_work_unit(0)->err_code_,
               OB_ERR_UNEXPECTED);
  ASSERT_EQ(OB_SUCCESS,
            ctx.check_work_unit_ready(*ctx.get_work_unit(1), is_ready));
  EXPECT_FALSE(is_ready);
}

TEST_F(ObDASFusionParallelCtxTest, cancel_unsubmitted_work_units)
{
  ObDASFusionParallelCtx ctx;
  ObDASFusionPathSliceWork works[3];
  for (int64_t i = 0; i < ARRAYSIZEOF(works); ++i) {
    works[i].exec_env_.path_idx_ = i;
    ASSERT_EQ(OB_SUCCESS,
              works[i].work_slice_.init_whole_path(i));
    ASSERT_EQ(OB_SUCCESS, ctx.works_.push_back(&works[i]));
  }
  ASSERT_EQ(OB_SUCCESS, ctx.build_work_units());
  ObDASFusionParallelCoordinator coordinator;
  ASSERT_EQ(OB_SUCCESS, coordinator.init(ARRAYSIZEOF(works)));

  ATOMIC_STORE(&ctx.get_work_unit(0)->submitted_, true);
  ctx.cancel_unsubmitted_work_units(coordinator, OB_TIMEOUT);

  EXPECT_FALSE(ctx.get_work_unit(0)->finished_);
  for (int64_t i = 1; i < ARRAYSIZEOF(works); ++i) {
    EXPECT_TRUE(ctx.get_work_unit(i)->finished_);
    EXPECT_EQ(OB_TIMEOUT, ctx.get_work_unit(i)->err_code_);
  }
  EXPECT_EQ(2, coordinator.finished_cnt_);
  EXPECT_EQ(OB_TIMEOUT, coordinator.get_first_error());
}

// ============================================================================
// 第 7 节：ObDASFusionWorkUnitTask 单测
// ============================================================================

class ObDASFusionWorkUnitTaskTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObDASFusionWorkUnitTaskTest, default_constructor)
{
  ObDASFusionWorkUnitTask task;
  EXPECT_EQ(nullptr, task.work_unit_);
  EXPECT_EQ(nullptr, task.coordinator_);
  EXPECT_EQ(INT64_MAX, task.timeout_ts_);
}

TEST_F(ObDASFusionWorkUnitTaskTest, init_with_null_work_unit)
{
  ObDASFusionWorkUnitTask task;
  ObDASFusionParallelCoordinator coord;
  EXPECT_EQ(OB_INVALID_ARGUMENT, task.init(nullptr, &coord, INT64_MAX, 0));
}

TEST_F(ObDASFusionWorkUnitTaskTest, init_with_null_coordinator)
{
  ObDASFusionWorkUnitTask task;
  ObDASFusionWorkUnit work_unit;
  TestFusionWork work(
      0, ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE);
  ASSERT_EQ(OB_SUCCESS,
            work_unit.init(0, 0, work));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            task.init(&work_unit, nullptr, INT64_MAX, 0));
}

TEST_F(ObDASFusionWorkUnitTaskTest, init_with_null_work)
{
  ObDASFusionWorkUnitTask task;
  ObDASFusionParallelCoordinator coord;
  ObDASFusionWorkUnit work_unit;
  work_unit.unit_id_ = 0;
  work_unit.path_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            task.init(&work_unit, &coord, INT64_MAX, 0));
}

TEST_F(ObDASFusionWorkUnitTaskTest, init_success)
{
  ObDASFusionWorkUnitTask task;
  ObDASFusionWorkUnit work_unit;
  TestFusionWork work(
      0, ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE);
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));
  ASSERT_EQ(OB_SUCCESS,
            work_unit.init(0, 0, work));

  ASSERT_EQ(OB_SUCCESS, task.init(&work_unit, &coord, 12345, 7));
  EXPECT_EQ(&work_unit, task.get_work_unit());
  EXPECT_EQ(&coord, task.get_coordinator());
  EXPECT_EQ(12345, task.get_timeout_ts());
}

TEST_F(ObDASFusionWorkUnitTaskTest, reset)
{
  ObDASFusionWorkUnitTask task;
  ObDASFusionWorkUnit work_unit;
  TestFusionWork work(
      0, ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE);
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));
  ASSERT_EQ(OB_SUCCESS,
            work_unit.init(0, 0, work));
  ASSERT_EQ(OB_SUCCESS, task.init(&work_unit, &coord, 12345, 7));

  task.reset();
  EXPECT_EQ(nullptr, task.work_unit_);
  EXPECT_EQ(nullptr, task.coordinator_);
  EXPECT_EQ(INT64_MAX, task.timeout_ts_);
}

// ============================================================================
// 第 8 节：ObDASFusionWorkUnitTaskHandler 单测
// ============================================================================

TEST_F(ObDASFusionWorkUnitTaskTest, handler_init_null_task)
{
  ObDASFusionWorkUnitTaskHandler handler;
  EXPECT_EQ(OB_INVALID_ARGUMENT, handler.init(nullptr));
}

TEST_F(ObDASFusionWorkUnitTaskTest, handler_init_success)
{
  ObDASFusionWorkUnitTask task;
  ObDASFusionWorkUnitTaskHandler handler;
  EXPECT_EQ(OB_SUCCESS, handler.init(&task));
}

// ============================================================================
// Section 9: Materialized Row Collection Tests
// ============================================================================

TEST(MaterializedRowCollectionTest, push_and_iterate)
{
  ObSEArray<ObDASFusionMaterializedRow, 256> rows;
  for (uint64_t i = 0; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows.push_back(ObDASFusionMaterializedRow(i, static_cast<double>(i) * 0.1)));
  }
  EXPECT_EQ(100, rows.count());

  for (int64_t i = 0; i < rows.count(); ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i), rows.at(i).rowkey_);
    EXPECT_DOUBLE_EQ(static_cast<double>(i) * 0.1, rows.at(i).score_);
  }
}

TEST(MaterializedRowCollectionTest, reserve_capacity)
{
  ObSEArray<ObDASFusionMaterializedRow, 256> rows;
  ASSERT_EQ(OB_SUCCESS, rows.reserve(1000));

  for (uint64_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows.push_back(ObDASFusionMaterializedRow(i, 1.0)));
  }
  EXPECT_EQ(1000, rows.count());
}

// ============================================================================
// 第 10 节：Coordinator 多线程集成单测
// ============================================================================

class ParallelIntegrationTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ParallelIntegrationTest, coordinator_stress_test)
{
  // Test with many concurrent finishes
  ObDASFusionParallelCoordinator coord;
  const int64_t N = 32;
  ASSERT_EQ(OB_SUCCESS, coord.init(N));

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&coord]() {
      coord.on_work_unit_finish(OB_SUCCESS);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 10000000;  // 10 seconds
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(timeout_ts));

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(ParallelIntegrationTest, coordinator_mixed_errors_stress)
{
  ObDASFusionParallelCoordinator coord;
  const int64_t N = 16;
  ASSERT_EQ(OB_SUCCESS, coord.init(N));

  std::vector<std::thread> threads;
  std::atomic<int> error_count(0);

  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&coord, &error_count, i]() {
      int err = OB_SUCCESS;
      if (i % 5 == 0) {
        err = OB_CANCELED;
        error_count.fetch_add(1);
      }
      coord.on_work_unit_finish(err);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 10000000;
  int ret = coord.wait_all_complete(timeout_ts);

  for (auto &t : threads) {
    t.join();
  }

  if (error_count.load() > 0) {
    EXPECT_EQ(OB_CANCELED, ret);
  } else {
    EXPECT_EQ(OB_SUCCESS, ret);
  }
}

// ============================================================================
// Section 11: ObDASFusionParallelCtx - release tests
// ============================================================================

TEST_F(ObDASFusionParallelCtxTest, release_empty_ctx)
{
  // Releasing an empty context should not crash
  ObDASFusionParallelCtx ctx;
  ctx.release();
  EXPECT_EQ(0, ctx.get_path_slice_work_count());
}

TEST_F(ObDASFusionParallelCtxTest, get_path_slice_work_returns_null_for_invalid_index)
{
  ObDASFusionParallelCtx ctx;
  EXPECT_EQ(nullptr, ctx.get_path_slice_work(-1));
  EXPECT_EQ(nullptr, ctx.get_path_slice_work(0));
  EXPECT_EQ(nullptr, ctx.get_path_slice_work(100));
}

TEST_F(ObDASFusionParallelCtxTest, has_shared_bitmaps_false_by_default)
{
  ObDASFusionParallelCtx ctx;
  EXPECT_FALSE(ctx.has_shared_bitmaps());
  EXPECT_EQ(0, ctx.get_shared_bitmap_count());
}

TEST_F(ObDASFusionParallelCtxTest, get_shared_bitmap_slot_invalid)
{
  ObDASFusionParallelCtx ctx;
  EXPECT_EQ(nullptr, ctx.get_shared_bitmap_slot(-1));
  EXPECT_EQ(nullptr, ctx.get_shared_bitmap_slot(0));
}

// ============================================================================
// Section 12: Edge case tests for coordinator wait semantics
// ============================================================================

TEST_F(ObDASFusionParallelCoordinatorTest, wait_indefinitely_when_timeout_is_max)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  // Finish immediately, then wait with INT64_MAX
  coord.on_work_unit_finish(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, multiple_errors_only_first_preserved)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(5));

  // All finish with different errors
  coord.on_work_unit_finish(OB_TIMEOUT);
  coord.on_work_unit_finish(OB_ERR_UNEXPECTED);
  coord.on_work_unit_finish(OB_CANCELED);
  coord.on_work_unit_finish(OB_ALLOCATE_MEMORY_FAILED);
  coord.on_work_unit_finish(OB_SUCCESS);

  // First error (OB_TIMEOUT) should win
  EXPECT_EQ(OB_TIMEOUT, coord.wait_all_complete(INT64_MAX));
}

// ============================================================================
// 第 13 节：ObDASFusionPathSliceWork 结果行累积单测
// ============================================================================

TEST(ObDASFusionPathSliceWorkTest, rows_accumulate_correctly)
{
  ObDASFusionPathSliceWork work;

  // Simulate worker thread pushing materialized rows
  for (uint64_t i = 0; i < 500; ++i) {
    ASSERT_EQ(OB_SUCCESS,
              work.rows_.push_back(
                  ObDASFusionMaterializedRow(
                      i, static_cast<double>(i))));
  }

  EXPECT_EQ(500, work.rows_.count());

  // Verify first and last
  EXPECT_EQ(0UL, work.rows_.at(0).rowkey_);
  EXPECT_DOUBLE_EQ(0.0, work.rows_.at(0).score_);
  EXPECT_EQ(499UL, work.rows_.at(499).rowkey_);
  EXPECT_DOUBLE_EQ(499.0, work.rows_.at(499).score_);

  // reset_result should clear
  work.reset_result();
  EXPECT_EQ(0, work.rows_.count());
}

// ============================================================================
// Section 14: ObDASFusionIter parallel/serial path switch test
// ============================================================================

class ObDASFusionIterParallelSwitchTest : public ::testing::Test
{
protected:
  ObDASFusionIterParallelSwitchTest()
      : alloc_("TestSwitch"),
        scan_alloc_("MockScan"),
        exec_ctx_(alloc_),
        eval_ctx_(exec_ctx_),
        ctdef_(alloc_),
        mock_scan_op_(scan_alloc_),
        search_ctx_(alloc_, mock_scan_op_)
  {
    eval_ctx_.max_batch_size_ = 256;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }

  ObArenaAllocator alloc_;
  ObArenaAllocator scan_alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASFusionCtDef ctdef_;
  ObDASScanOp mock_scan_op_;
  ObDASSearchCtx search_ctx_;
};

TEST_F(ObDASFusionIterParallelSwitchTest,
       inner_init_does_not_select_parallel_execution)
{
  ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef_.enable_parallel_ = true;
  ctdef_.children_cnt_ = 2;
  ctdef_.query_dop_ = 1;
  ctdef_.has_search_subquery_ = true;
  ctdef_.has_vector_subquery_ = true;

  // Setup minimal expressions to avoid nullptr crashes
  ObExpr rowid_expr, score0, score1, fusion_score;
  ctdef_.rowid_exprs_.init(1);
  ctdef_.rowid_exprs_.push_back(&rowid_expr);
  ctdef_.score_exprs_.init(3);
  ctdef_.score_exprs_.push_back(&score0);
  ctdef_.score_exprs_.push_back(&score1);
  ctdef_.score_exprs_.push_back(&fusion_score);
  ctdef_.result_output_.init(4);
  ctdef_.result_output_.push_back(&rowid_expr);
  ctdef_.result_output_.push_back(&score0);
  ctdef_.result_output_.push_back(&score1);
  ctdef_.result_output_.push_back(&fusion_score);

  // Minimal expr setup to pass set_rowkey_is_uint64_flag
  rowid_expr.obj_meta_.set_type(ObUInt64Type);

  ObDASFusionIter iter;
  ObDASFusionIterParam param;
  param.type_ = ObDASIterType::DAS_ITER_FUSION;
  param.fusion_ctdef_ = &ctdef_;
  param.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  param.rank_window_size_ = 10;
  param.size_ = 10;
  param.eval_ctx_ = &eval_ctx_;
  param.exec_ctx_ = &exec_ctx_;
  param.max_size_ = 1024;
  param.output_ = &ctdef_.result_output_;
  param.weights_.push_back(0.5);
  param.weights_.push_back(0.5);
  param.path_top_k_limits_.push_back(10);
  param.path_top_k_limits_.push_back(10);
  param.search_ctx_ = &search_ctx_;

  ASSERT_EQ(OB_SUCCESS, iter.init(param));
  EXPECT_FALSE(iter.parallel_execution_used_);

  iter.release();
}

TEST_F(ObDASFusionIterParallelSwitchTest,
       inner_init_keeps_execution_unselected_when_parallel_disabled)
{
  ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef_.enable_parallel_ = false;
  ctdef_.children_cnt_ = 2;
  ctdef_.has_search_subquery_ = true;
  ctdef_.has_vector_subquery_ = true;

  ObExpr rowid_expr, score0, score1, fusion_score;
  ctdef_.rowid_exprs_.init(1);
  ctdef_.rowid_exprs_.push_back(&rowid_expr);
  ctdef_.score_exprs_.init(3);
  ctdef_.score_exprs_.push_back(&score0);
  ctdef_.score_exprs_.push_back(&score1);
  ctdef_.score_exprs_.push_back(&fusion_score);
  ctdef_.result_output_.init(4);
  ctdef_.result_output_.push_back(&rowid_expr);
  ctdef_.result_output_.push_back(&score0);
  ctdef_.result_output_.push_back(&score1);
  ctdef_.result_output_.push_back(&fusion_score);

  rowid_expr.obj_meta_.set_type(ObUInt64Type);

  ObDASFusionIter iter;
  ObDASFusionIterParam param;
  param.type_ = ObDASIterType::DAS_ITER_FUSION;
  param.fusion_ctdef_ = &ctdef_;
  param.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  param.rank_window_size_ = 10;
  param.size_ = 10;
  param.eval_ctx_ = &eval_ctx_;
  param.exec_ctx_ = &exec_ctx_;
  param.max_size_ = 1024;
  param.output_ = &ctdef_.result_output_;
  param.weights_.push_back(0.5);
  param.weights_.push_back(0.5);
  param.path_top_k_limits_.push_back(10);
  param.path_top_k_limits_.push_back(10);
  param.search_ctx_ = &search_ctx_;

  ASSERT_EQ(OB_SUCCESS, iter.init(param));
  EXPECT_FALSE(iter.parallel_execution_used_);

  iter.release();
}

// ============================================================================
// Section 15: ObDASFusionIter merge_parallel_results Tests
// ============================================================================

class ObDASFusionIterMergeTest : public ::testing::Test
{
protected:
  ObDASFusionIterMergeTest()
      : alloc_("TestMerge"),
        exec_ctx_(alloc_),
        eval_ctx_(exec_ctx_),
        ctdef_(alloc_)
  {
    eval_ctx_.max_batch_size_ = 256;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;

    // Setup frames for expression evaluation
    const int64_t frame_size = 1024 * 1024;  // 1MB
    exec_ctx_.frames_ = (char **)alloc_.alloc(2 * sizeof(char*));
    if (OB_NOT_NULL(exec_ctx_.frames_)) {
      exec_ctx_.frames_[0] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[0])) {
        memset(exec_ctx_.frames_[0], 0, frame_size);
      }
      exec_ctx_.frames_[1] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[1])) {
        memset(exec_ctx_.frames_[1], 0, frame_size);
      }
      exec_ctx_.frame_cnt_ = 2;
    }
    eval_ctx_.frames_ = exec_ctx_.frames_;
  }

  ObArenaAllocator alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASFusionCtDef ctdef_;
};

TEST_F(ObDASFusionIterMergeTest, merge_with_null_fusion_ctdef)
{
  ObDASFusionIter iter;
  ObDASFusionPathSliceWork work;
  work.exec_env_.path_idx_ = 0;
  iter.parallel_ctx_.path_slice_works_.push_back(&work);

  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.merge_parallel_results());

  // Clean up - remove from array without destroying (stack allocated)
  iter.parallel_ctx_.path_slice_works_.reset();
}

TEST(ObDASFusionWorkUnitTest, scheduling_error_state_belongs_to_work_unit)
{
  TestFusionWork work(
      0, ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE);
  ObDASFusionWorkUnit work_unit;
  ASSERT_EQ(OB_SUCCESS, work_unit.init(0, 0, work));
  ATOMIC_STORE(&work_unit.submitted_, true);
  ATOMIC_STORE(&work_unit.err_code_, OB_CANCELED);
  ATOMIC_STORE(&work_unit.finished_, true);
  EXPECT_TRUE(work_unit.submitted_);
  EXPECT_TRUE(work_unit.finished_);
  EXPECT_EQ(OB_CANCELED, work_unit.err_code_);
}

// ============================================================================
// Section 16: release_parallel_runtime Tests
// ============================================================================

TEST_F(ObDASFusionIterMergeTest, release_parallel_runtime_when_not_inited)
{
  // Calling release_parallel_runtime when coordinator is not inited should be safe
  ObDASFusionIter iter;
  EXPECT_FALSE(iter.parallel_coordinator_.is_inited());
  iter.release_parallel_runtime();  // Should not crash
}

// ============================================================================
// Section 17: Bitmap discovery helper tests (static functions)
// ============================================================================
// Note: discover_bitmap_ops_dfs and find_bitmap_op_by_dfs_idx are file-static
// (anonymous namespace) in ob_das_fusion_parallel.cpp, so we test them indirectly
// through ObDASFusionParallelCtx::discover_shared_bitmaps.

TEST_F(ObDASFusionParallelCtxTest, discover_shared_bitmaps_null_iter)
{
  ObDASFusionParallelCtx ctx;
  // Should be safe with null iter
  EXPECT_EQ(OB_SUCCESS, ctx.discover_shared_bitmaps(alloc_, nullptr, 4));
  EXPECT_FALSE(ctx.has_shared_bitmaps());
}

TEST_F(ObDASFusionParallelCtxTest, discover_shared_bitmaps_dop_1)
{
  ObDASFusionParallelCtx ctx;
  // dop <= 1 means no range parallel, skip discovery
  int dummy = 0;
  ObDASIter *fake_iter = reinterpret_cast<ObDASIter *>(&dummy);
  EXPECT_EQ(OB_SUCCESS, ctx.discover_shared_bitmaps(alloc_, fake_iter, 1));
  EXPECT_FALSE(ctx.has_shared_bitmaps());
}

TEST_F(ObDASFusionParallelCtxTest,
       discover_shared_bitmaps_skips_async_segment_local_bitmap)
{
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  ObDASBitmapOp bitmap_op(search_ctx);
  TestEstimatedWorkSearchOp segment_merge(search_ctx, 0);
  ObIDASSearchOp *segment_children[] = {&bitmap_op};
  ObDASSearchDriverIter search_iter;
  ObDASFusionParallelCtx ctx;

  bitmap_op.op_type_ = DAS_SEARCH_OP_BITMAP;
  segment_merge.set_work_role(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, false);
  segment_merge.set_children(segment_children, ARRAYSIZEOF(segment_children));
  search_iter.set_root_op(&segment_merge);

  ASSERT_EQ(OB_SUCCESS, ctx.discover_shared_bitmaps(alloc_, &search_iter, 2));
  EXPECT_FALSE(ctx.has_shared_bitmaps());
  EXPECT_EQ(0, ctx.get_shared_bitmap_count());
}

TEST_F(ObDASFusionParallelCtxTest,
       discover_shared_bitmaps_registers_filter_provider_explicitly)
{
  ObDASAsyncIndexQueryCtx catalog;
  ASSERT_EQ(OB_SUCCESS, build_filter_provider_test_catalog(catalog));
  const ObAsyncIndexPartitionSegCtx *partition = nullptr;
  ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(2), partition));
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  search_ctx.set_async_query_ctx(&catalog);
  search_ctx.set_current_async_partition(partition);
  TestEstimatedWorkSearchOp root_op(search_ctx, 0);
  TestEstimatedWorkSearchOp source_op(search_ctx, 0);
  TestEstimatedWorkSearchOp second_source_op(search_ctx, 0);
  ObDASSearchDriverIter search_iter;
  ObDASFusionParallelCtx ctx;
  ObDASFilterBitmapProvider *provider = nullptr;
  ObDASFilterBitmapProvider *second_provider = nullptr;
  const ObIDASSearchRtDef *owner =
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234);

  root_op.set_work_role(DAS_SEARCH_OP_DUMMY, false);
  source_op.set_work_role(DAS_SEARCH_OP_DUMMY, false);
  second_source_op.set_work_role(DAS_SEARCH_OP_DUMMY, false);
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 0, &source_op, provider));
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 1, &second_source_op,
      second_provider));
  ASSERT_NE(nullptr, provider);
  ASSERT_NE(nullptr, second_provider);
  ASSERT_NE(provider, second_provider);
  search_iter.set_root_op(&root_op);
  search_iter.set_search_ctx(&search_ctx);

  ASSERT_EQ(OB_SUCCESS,
            ctx.discover_shared_bitmaps(alloc_, &search_iter, 2));
  ASSERT_EQ(2, ctx.get_shared_bitmap_count());
  EXPECT_EQ(0, ctx.get_shared_bitmap_op_count());
  for (int64_t i = 0; i < 2; ++i) {
    ObSharedBitmapSlot *slot = ctx.get_shared_bitmap_slot(i);
    ASSERT_NE(nullptr, slot);
    EXPECT_EQ(-1, slot->bitmap_occurrence_idx_);
    EXPECT_EQ(i, slot->filter_provider_idx_);
  }
}

TEST_F(ObDASFusionParallelCtxTest,
       filter_provider_prepare_rejects_old_inactive_group_slot)
{
  ObDASAsyncIndexQueryCtx catalog;
  ASSERT_EQ(OB_SUCCESS, build_filter_provider_test_catalog(catalog));
  const ObAsyncIndexPartitionSegCtx *partition = nullptr;
  ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(1), partition));
  ObArenaAllocator scan_alloc("InactiveOld");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc_);
  ObEvalCtx eval_ctx(exec_ctx);
  eval_ctx.max_batch_size_ = 8;
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  search_ctx.eval_ctx_ = &eval_ctx;
  search_ctx.set_async_query_ctx(&catalog);
  search_ctx.set_current_async_partition(partition);
  TestEnumerableSearchOp leaf(search_ctx, std::vector<uint64_t>{11});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&leaf));
  ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(9002, 1));
  param.set_is_scoring(false);
  ObDASAsyncSegmentMergeOp *source = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_op(param, source));
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_GROUP_SCOPE, 1, source, provider));
  // Model the old discovery result: every registered Provider became a slot,
  // including Group 1 which does not exist in the selected partition.
  void *buf = alloc_.alloc(sizeof(ObSharedBitmapSlot));
  ASSERT_NE(nullptr, buf);
  ObSharedBitmapSlot *slot = new (buf) ObSharedBitmapSlot();
  slot->filter_provider_idx_ = 0;
  ObDASFusionParallelCtx parallel_ctx;
  ASSERT_EQ(OB_SUCCESS, parallel_ctx.shared_bitmap_slots_.push_back(slot));
  ObDASFusionParallelCoordinator coordinator;
  ASSERT_EQ(OB_SUCCESS, coordinator.init(1));
  ObDASFusionBitmapPrepareWork prepare_work;
  prepare_work.parallel_ctx_ = &parallel_ctx;
  prepare_work.exec_env_.child_search_ctx_ = &search_ctx;
  EXPECT_EQ(OB_ERR_UNEXPECTED, prepare_work.build_shared_bitmaps(INT64_MAX, coordinator));
  EXPECT_TRUE(ATOMIC_LOAD(&slot->is_built_));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ATOMIC_LOAD(&slot->build_ret_));
  EXPECT_EQ(nullptr, slot->bitmap_);
  EXPECT_FALSE(provider->is_prepared());
  parallel_ctx.release();
  search_ctx.reset();
}

TEST_F(ObDASFusionParallelCtxTest,
       filter_provider_prepare_skips_inactive_groups_preserving_ordinals)
{
  ObDASAsyncIndexQueryCtx catalog;
  ASSERT_EQ(OB_SUCCESS, build_filter_provider_test_catalog(catalog));
  ASSERT_EQ(2, catalog.get_partition_cnt());
  ASSERT_EQ(2, catalog.get_max_group_cnt());
  ASSERT_EQ(2, catalog.get_max_segment_cnt(1, 9001));

  // Each partition execution gets fresh PREPARE/slot state; the immutable
  // catalog and maximum Group template stay the same across 1 -> 2 -> 1.
  const int64_t partition_ids[] = {1, 2, 1};
  for (int64_t phase = 0; phase < ARRAYSIZEOF(partition_ids); ++phase) {
    const bool group1_active = 2 == partition_ids[phase];
    const ObAsyncIndexPartitionSegCtx *partition = nullptr;
    ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(partition_ids[phase]), partition));
    ASSERT_NE(nullptr, partition);
    ASSERT_EQ(group1_active ? 2 : 1, partition->get_storage_group_ctx().get_group_count());
    ObArenaAllocator scan_alloc("InactiveScan");
    ObDASScanOp mock_scan_op(scan_alloc);
    ObExecContext exec_ctx(alloc_);
    ObEvalCtx eval_ctx(exec_ctx);
    eval_ctx.max_batch_size_ = 8;
    ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
    search_ctx.eval_ctx_ = &eval_ctx;
    search_ctx.set_async_query_ctx(&catalog);
    search_ctx.set_current_async_partition(partition);
    TestEstimatedWorkSearchOp root_op(search_ctx, 0);
    root_op.set_work_role(DAS_SEARCH_OP_DUMMY, false);
    TestEnumerableSearchOp query_source(search_ctx, std::vector<uint64_t>{2, 7, 11});
    TestEnumerableSearchOp group0_source(search_ctx, std::vector<uint64_t>{2, 7});
    TestEnumerableSearchOp group1_leaf(search_ctx, std::vector<uint64_t>{11});
    ObArray<ObIDASSearchOp *> source_children;
    ASSERT_EQ(OB_SUCCESS, source_children.push_back(&group1_leaf));
    ObDASAsyncSegmentMergeOpParam source_param(
        source_children, ObDASAsyncSegmentDomain(9002, 1));
    source_param.set_is_scoring(false);
    ObDASAsyncSegmentMergeOp *group1_source = nullptr;
    ASSERT_EQ(OB_SUCCESS, search_ctx.create_op(source_param, group1_source));
    ASSERT_NE(nullptr, group1_source);
    // This is the real Source binding check that the old unconditional
    // PREPARE hit for a Group absent from the current partition.
    EXPECT_EQ(group1_active ? OB_SUCCESS : OB_ERR_UNEXPECTED,
              group1_source->load_current_partition_segments());
    const ObIDASSearchRtDef *owner =
        reinterpret_cast<const ObIDASSearchRtDef *>(0x1234);
    ObDASFilterBitmapProvider *query_provider = nullptr;
    ObDASFilterBitmapProvider *group1_provider = nullptr;
    ObDASFilterBitmapProvider *group0_provider = nullptr;
    ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
        owner, DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
        &query_source, query_provider));
    ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
        owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 1, group1_source, group1_provider));
    ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
        owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 0, &group0_source, group0_provider));
    ASSERT_EQ(3, search_ctx.get_filter_bitmap_provider_count());
    ObDASSearchDriverIter search_iter;
    search_iter.set_root_op(&root_op);
    search_iter.set_search_ctx(&search_ctx);
    ObDASFusionParallelCtx parallel_ctx;
    ASSERT_EQ(OB_SUCCESS, parallel_ctx.discover_shared_bitmaps(alloc_, &search_iter, 4));
    ASSERT_EQ(group1_active ? 3 : 2, parallel_ctx.get_shared_bitmap_count());
    EXPECT_EQ(0, parallel_ctx.get_shared_bitmap_slot(0)->filter_provider_idx_);
    EXPECT_EQ(2, parallel_ctx.get_shared_bitmap_slot(
        parallel_ctx.get_shared_bitmap_count() - 1)->filter_provider_idx_);
    EXPECT_EQ(3, search_ctx.get_filter_bitmap_provider_count());
    ObDASFusionBitmapPrepareWork prepare_work;
    ObDASFusionParallelCoordinator coordinator;
    ASSERT_EQ(OB_SUCCESS, coordinator.init(1));
    prepare_work.parallel_ctx_ = &parallel_ctx;
    prepare_work.exec_env_.child_search_ctx_ = &search_ctx;
    ASSERT_EQ(OB_SUCCESS, prepare_work.build_shared_bitmaps(INT64_MAX, coordinator));
    EXPECT_TRUE(query_provider->is_prepared());
    EXPECT_TRUE(group0_provider->is_prepared());
    EXPECT_EQ(group1_active, group1_provider->is_prepared());
    for (int64_t i = 0; i < parallel_ctx.get_shared_bitmap_count(); ++i) {
      ObSharedBitmapSlot *slot = parallel_ctx.get_shared_bitmap_slot(i);
      EXPECT_TRUE(ATOMIC_LOAD(&slot->is_built_));
      EXPECT_EQ(OB_SUCCESS, ATOMIC_LOAD(&slot->build_ret_));
      ASSERT_NE(nullptr, slot->bitmap_);
    }
    parallel_ctx.release();
    search_ctx.reset();
  }
}

// ============================================================================
// The former compaction mysqltests tried to produce these asymmetric catalogs
// with background merge timing. Keep discovery/PREPARE contracts deterministic.
TEST_F(ObDASFusionParallelCtxTest, ordinary_bitmap_prepare_uses_only_current_partition_groups)
{
  // Empty scan isolates whether PREPARE opens an inactive source. Bitmap values
  // and batching are independently checked by ObDASBitmapOpBuildTest.
  class CountingEmptySource : public TestEnumerableSearchOp
  {
  public:
    explicit CountingEmptySource(ObDASSearchCtx &ctx)
      : TestEnumerableSearchOp(ctx, {}), open_count_(0) {}
    int64_t open_count_;
  protected:
    int do_open() override { ++open_count_; return OB_SUCCESS; }
    int do_next_rowids(const int64_t, int64_t &count) override
    { count = 0; return OB_ITER_END; }
  };
  ObDASAsyncIndexQueryCtx catalog;
  ASSERT_EQ(OB_SUCCESS, build_filter_provider_test_catalog(catalog));
  const int64_t partition_ids[] = {1, 2, 1};
  for (int64_t partition_id : partition_ids) {
    SCOPED_TRACE(partition_id);
    const ObAsyncIndexPartitionSegCtx *partition = nullptr;
    ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(partition_id), partition));
    const int64_t active_count = 2 == partition_id ? 2 : 1;
    ASSERT_EQ(active_count, partition->get_storage_group_ctx().get_group_count());
    ObDASScanOp scan_op(alloc_);
    ObExecContext exec_ctx(alloc_);
    ObEvalCtx eval_ctx(exec_ctx);
    eval_ctx.max_batch_size_ = 8;
    ObDASSearchCtx ctx(alloc_, scan_op);
    ctx.eval_ctx_ = &eval_ctx;
    ctx.table_row_count_ = ObDASSearchCost(0);
    ctx.set_async_query_ctx(&catalog);
    ctx.set_current_async_partition(partition);
    CountingEmptySource first_source(ctx), second_source(ctx);
    ObDASBitmapOp first(ctx), second(ctx);
    first.op_type_ = second.op_type_ = DAS_SEARCH_OP_BITMAP;
    first.input_op_ = &first_source;
    second.input_op_ = &second_source;
    ObArray<ObIDASSearchOp *> children;
    ASSERT_EQ(OB_SUCCESS, children.push_back(&first));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&second));
    ObDASAsyncSegmentGroupMergeOpParam param(children);
    ObDASAsyncSegmentGroupMergeOp root(ctx);
    ASSERT_EQ(OB_SUCCESS, root.init(param));
    ObDASSearchDriverIter driver;
    driver.set_root_op(&root);
    driver.set_search_ctx(&ctx);
    ObDASFusionParallelCtx parallel;
    ASSERT_EQ(OB_SUCCESS, parallel.discover_shared_bitmaps(alloc_, &driver, 4));
    ASSERT_EQ(active_count, parallel.get_shared_bitmap_count());
    ObDASFusionBitmapPrepareWork work;
    work.parallel_ctx_ = &parallel;
    ASSERT_EQ(OB_SUCCESS, work.child_bitmap_ops_.push_back(&first));
    ASSERT_EQ(OB_SUCCESS, work.child_bitmap_ops_.push_back(&second));
    ObDASFusionParallelCoordinator coordinator;
    ASSERT_EQ(OB_SUCCESS, coordinator.init(1));
    ASSERT_EQ(OB_SUCCESS, work.build_shared_bitmaps(INT64_MAX, coordinator));
    EXPECT_EQ(1, first_source.open_count_);
    EXPECT_EQ(active_count - 1, second_source.open_count_);
    for (int64_t i = 0; i < active_count; ++i) {
      const ObSharedBitmapSlot *slot = parallel.get_shared_bitmap_slot(i);
      EXPECT_EQ(i, slot->bitmap_occurrence_idx_);
      EXPECT_TRUE(slot->is_built_);
      EXPECT_EQ(OB_SUCCESS, slot->build_ret_);
      ASSERT_NE(nullptr, slot->bitmap_);
      EXPECT_EQ(0, slot->bitmap_->cardinality());
    }
    parallel.release();
  }
}

TEST_F(ObDASFusionParallelCtxTest, provider_only_in_high_group_needs_no_prepare_in_short_partition)
{
  ObDASAsyncIndexQueryCtx catalog;
  ASSERT_EQ(OB_SUCCESS, build_filter_provider_test_catalog(catalog));
  const int64_t partition_ids[] = {1, 2, 1};
  for (int64_t partition_id : partition_ids) {
    SCOPED_TRACE(partition_id);
    const bool active = 2 == partition_id;
    const ObAsyncIndexPartitionSegCtx *partition = nullptr;
    ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(partition_id), partition));
    ObDASScanOp scan_op(alloc_);
    ObExecContext exec_ctx(alloc_);
    ObEvalCtx eval_ctx(exec_ctx);
    eval_ctx.max_batch_size_ = 8;
    ObDASSearchCtx ctx(alloc_, scan_op);
    ctx.eval_ctx_ = &eval_ctx;
    ctx.set_async_query_ctx(&catalog);
    ctx.set_current_async_partition(partition);
    TestEnumerableSearchOp leaf(ctx, {11});
    ObArray<ObIDASSearchOp *> children;
    ASSERT_EQ(OB_SUCCESS, children.push_back(&leaf));
    ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(9002, 1));
    ObDASAsyncSegmentMergeOp *source = nullptr;
    ASSERT_EQ(OB_SUCCESS, ctx.create_op(param, source));
    ObDASFilterBitmapProvider *provider = nullptr;
    ASSERT_EQ(OB_SUCCESS, ctx.create_filter_bitmap_provider(
        reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
        DAS_FILTER_BITMAP_GROUP_SCOPE, 1, source, provider));
    TestEnumerableSearchOp root(ctx, {});
    ObDASSearchDriverIter driver;
    driver.set_root_op(&root);
    driver.set_search_ctx(&ctx);
    ObDASFusionParallelCtx parallel;
    ASSERT_EQ(OB_SUCCESS, parallel.discover_shared_bitmaps(alloc_, &driver, 4));
    ASSERT_EQ(active ? 1 : 0, parallel.get_shared_bitmap_count());
    EXPECT_EQ(active, parallel.has_shared_bitmaps());
    EXPECT_EQ(1, ctx.get_filter_bitmap_provider_count());
    ObDASFusionBitmapPrepareWork work;
    work.parallel_ctx_ = &parallel;
    work.exec_env_.child_search_ctx_ = &ctx;
    ObDASFusionParallelCoordinator coordinator;
    ASSERT_EQ(OB_SUCCESS, coordinator.init(1));
    ASSERT_EQ(OB_SUCCESS, work.build_shared_bitmaps(INT64_MAX, coordinator));
    EXPECT_EQ(active, provider->is_prepared());
    if (active) {
      const ObSharedBitmapSlot *slot = parallel.get_shared_bitmap_slot(0);
      EXPECT_EQ(0, slot->filter_provider_idx_);
      ASSERT_NE(nullptr, slot->bitmap_);
      EXPECT_EQ(1, slot->bitmap_->cardinality());
      ObFastBitmap::Iterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.init(slot->bitmap_));
      uint64_t id = 0;
      ASSERT_EQ(OB_SUCCESS, iter.next_id(id));
      EXPECT_EQ(11, id);
      EXPECT_EQ(OB_ITER_END, iter.next_id(id));
    }
    parallel.release();
    ctx.reset();
  }
}

// Section 18: FusionCtDef parallel config tests
// ============================================================================

TEST_F(ObDASFusionParallelCtxTest, ctdef_parallel_config)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.query_dop_ = 4;
  ctdef_.children_cnt_ = 3;
  ctdef_.has_search_subquery_ = true;

  EXPECT_TRUE(ctdef_.enable_parallel_);
  EXPECT_EQ(4, ctdef_.query_dop_);
}

TEST_F(ObDASFusionParallelCtxTest, ctdef_default_query_dop_is_auto)
{
  EXPECT_EQ(ObDASFusionDopPolicy::AUTO_QUERY_DOP, ctdef_.query_dop_);
}

TEST_F(ObDASFusionParallelCtxTest, ctdef_is_search_index)
{
  ctdef_.children_cnt_ = 3;
  ctdef_.has_search_subquery_ = true;
  ctdef_.set_search_index(1);

  EXPECT_TRUE(ctdef_.is_search_index(1));
  EXPECT_FALSE(ctdef_.is_search_index(0));
  EXPECT_FALSE(ctdef_.is_search_index(2));

  // When has_search_subquery_ is false, always returns false
  ctdef_.has_search_subquery_ = false;
  EXPECT_FALSE(ctdef_.is_search_index(1));
}

TEST_F(ObDASFusionParallelCtxTest, ctdef_is_vector_index)
{
  ctdef_.children_cnt_ = 3;
  ctdef_.has_vector_subquery_ = true;
  ctdef_.has_search_subquery_ = true;
  ctdef_.set_search_index(1);

  // Indices 0 and 2 are NOT the search index, so they are vector indices
  EXPECT_TRUE(ctdef_.is_vector_index(0));
  EXPECT_FALSE(ctdef_.is_vector_index(1));  // This is the search index
  EXPECT_TRUE(ctdef_.is_vector_index(2));

  // When has_vector_subquery_ is false, always returns false
  ctdef_.has_vector_subquery_ = false;
  EXPECT_FALSE(ctdef_.is_vector_index(0));
}

// ============================================================================
// Section 19: End-to-end simulation of parallel merge flow
// ============================================================================

class ParallelMergeSimulationTest : public ::testing::Test
{
protected:
  ParallelMergeSimulationTest()
      : alloc_("TestE2E"),
        exec_ctx_(alloc_),
        eval_ctx_(exec_ctx_),
        ctdef_(alloc_)
  {
    eval_ctx_.max_batch_size_ = 256;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;

    // Setup frames
    const int64_t frame_size = 1024 * 1024;
    exec_ctx_.frames_ = (char **)alloc_.alloc(2 * sizeof(char*));
    if (OB_NOT_NULL(exec_ctx_.frames_)) {
      exec_ctx_.frames_[0] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[0])) {
        memset(exec_ctx_.frames_[0], 0, frame_size);
      }
      exec_ctx_.frames_[1] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[1])) {
        memset(exec_ctx_.frames_[1], 0, frame_size);
      }
      exec_ctx_.frame_cnt_ = 2;
    }
    eval_ctx_.frames_ = exec_ctx_.frames_;

    // Setup ctdef
    ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
    ctdef_.children_cnt_ = 2;
    ctdef_.has_search_subquery_ = true;
    ctdef_.has_vector_subquery_ = true;
    ctdef_.set_search_index(0);

    // Expressions
    rowid_expr_.obj_meta_.set_type(ObUInt64Type);
    rowid_expr_.datum_meta_.type_ = ObUInt64Type;
    rowid_expr_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    rowid_expr_.frame_idx_ = 0;
    int64_t pos = 0;
    rowid_expr_.datum_off_ = pos; pos += sizeof(ObDatum);
    rowid_expr_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    rowid_expr_.res_buf_off_ = pos; rowid_expr_.res_buf_len_ = 8; pos += 8;
    rowid_expr_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    score0_.obj_meta_.set_type(ObDoubleType);
    score0_.datum_meta_.type_ = ObDoubleType;
    score0_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    score0_.frame_idx_ = 0;
    score0_.datum_off_ = pos; pos += sizeof(ObDatum);
    score0_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    score0_.res_buf_off_ = pos; score0_.res_buf_len_ = 8; pos += 8;
    score0_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    score1_.obj_meta_.set_type(ObDoubleType);
    score1_.datum_meta_.type_ = ObDoubleType;
    score1_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    score1_.frame_idx_ = 0;
    score1_.datum_off_ = pos; pos += sizeof(ObDatum);
    score1_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    score1_.res_buf_off_ = pos; score1_.res_buf_len_ = 8; pos += 8;
    score1_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    fusion_score_.obj_meta_.set_type(ObDoubleType);
    fusion_score_.datum_meta_.type_ = ObDoubleType;
    fusion_score_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    fusion_score_.frame_idx_ = 0;
    fusion_score_.datum_off_ = pos; pos += sizeof(ObDatum);
    fusion_score_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    fusion_score_.res_buf_off_ = pos; fusion_score_.res_buf_len_ = 8; pos += 8;
    fusion_score_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    ctdef_.rowid_exprs_.init(1);
    ctdef_.rowid_exprs_.push_back(&rowid_expr_);
    ctdef_.score_exprs_.init(3);
    ctdef_.score_exprs_.push_back(&score0_);
    ctdef_.score_exprs_.push_back(&score1_);
    ctdef_.score_exprs_.push_back(&fusion_score_);
    ctdef_.result_output_.init(4);
    ctdef_.result_output_.push_back(&rowid_expr_);
    ctdef_.result_output_.push_back(&score0_);
    ctdef_.result_output_.push_back(&score1_);
    ctdef_.result_output_.push_back(&fusion_score_);
  }

  ObArenaAllocator alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASFusionCtDef ctdef_;
  ObExpr rowid_expr_;
  ObExpr score0_;
  ObExpr score1_;
  ObExpr fusion_score_;
};

TEST_F(ParallelMergeSimulationTest, simulate_two_path_merge)
{
  // Simulate what happens after parallel tasks complete:
  // Path 0 (search): rows (1, 0.8), (2, 0.6)
  // Path 1 (vec):    rows (1, 0.7), (3, 0.9)
  // Expected after merge + fusion (weight_sum 0.5/0.5):
  //   doc 1: 0.8*0.5 + 0.7*0.5 = 0.75
  //   doc 2: 0.6*0.5 = 0.30
  //   doc 3: 0.9*0.5 = 0.45

  ObDASFusionIter iter;

  // 手工构造预先填充路径 Work 的 parallel_ctx_。
  ObDASFusionPathSliceWork rt0, rt1;
  rt0.exec_env_.path_idx_ = 0;
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 0.8));
  rt0.rows_.push_back(ObDASFusionMaterializedRow(2, 0.6));

  rt1.exec_env_.path_idx_ = 1;
  rt1.rows_.push_back(ObDASFusionMaterializedRow(1, 0.7));
  rt1.rows_.push_back(ObDASFusionMaterializedRow(3, 0.9));

  iter.parallel_ctx_.path_slice_works_.push_back(&rt0);
  iter.parallel_ctx_.path_slice_works_.push_back(&rt1);

  // Setup iter state
  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  // Create memctx for fusion_docs_
  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  // Call merge_parallel_results
  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());

  // Verify fusion_docs_
  EXPECT_EQ(3, iter.fusion_docs_.count());

  // Doc 1 should have both paths
  bool found_doc1 = false, found_doc2 = false, found_doc3 = false;
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    const ObDASFusionDocInfo &doc = iter.fusion_docs_.at(i);
    uint64_t rk = doc.get_uint64_rowkey();
    if (rk == 1) {
      found_doc1 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
      double s0 = 0, s1 = 0;
      ASSERT_EQ(OB_SUCCESS, doc.get_raw_score(0, s0));
      ASSERT_EQ(OB_SUCCESS, doc.get_raw_score(1, s1));
      EXPECT_DOUBLE_EQ(0.8, s0);
      EXPECT_DOUBLE_EQ(0.7, s1);
    } else if (rk == 2) {
      found_doc2 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_FALSE(doc.has_path(1));
    } else if (rk == 3) {
      found_doc3 = true;
      EXPECT_FALSE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
    }
  }
  EXPECT_TRUE(found_doc1);
  EXPECT_TRUE(found_doc2);
  EXPECT_TRUE(found_doc3);

  // Clean up
  iter.parallel_ctx_.path_slice_works_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ParallelMergeSimulationTest, simulate_range_parallel_merge)
{
  // 模拟 Range 并行合并：搜索路径 0 包含三个 Slice Work。
  // with different docid ranges. After merge, duplicates should be handled correctly.
  //
  // Range 0: (1, 0.9), (2, 0.8)
  // Range 1: (3, 0.7), (4, 0.6)
  // Range 2: (5, 0.5)

  ObDASFusionIter iter;

  ObDASFusionPathSliceWork rt0, rt1, rt2;
  rt0.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt0, 0, 0, 5);
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 0.9));
  rt0.rows_.push_back(ObDASFusionMaterializedRow(2, 0.8));

  rt1.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt1, 0, 1, 5);
  rt1.rows_.push_back(ObDASFusionMaterializedRow(3, 0.7));
  rt1.rows_.push_back(ObDASFusionMaterializedRow(4, 0.6));

  rt2.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt2, 0, 2, 5);
  rt2.rows_.push_back(ObDASFusionMaterializedRow(5, 0.5));

  iter.parallel_ctx_.path_slice_works_.push_back(&rt0);
  iter.parallel_ctx_.path_slice_works_.push_back(&rt1);
  iter.parallel_ctx_.path_slice_works_.push_back(&rt2);

  // Only 1 child (search path) in ctdef
  ObDASFusionCtDef single_ctdef(alloc_);
  single_ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  single_ctdef.children_cnt_ = 1;
  single_ctdef.has_search_subquery_ = true;
  single_ctdef.set_search_index(0);

  single_ctdef.rowid_exprs_.init(1);
  single_ctdef.rowid_exprs_.push_back(&rowid_expr_);
  single_ctdef.score_exprs_.init(2);
  single_ctdef.score_exprs_.push_back(&score0_);
  single_ctdef.score_exprs_.push_back(&fusion_score_);

  iter.fusion_ctdef_ = &single_ctdef;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 1;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());

  // All 5 rows merged into path 0, no duplicates
  EXPECT_EQ(5, iter.fusion_docs_.count());

  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    const ObDASFusionDocInfo &doc = iter.fusion_docs_.at(i);
    EXPECT_TRUE(doc.has_path(0));
  }

  // Clean up
  iter.parallel_ctx_.path_slice_works_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ParallelMergeSimulationTest, simulate_range_parallel_merge_respects_logical_path_topk)
{
  // The logical Query path has global top-2 = {(1, 100.0), (2, 99.0)}.
  // 每个 Range 并行 WorkUnit 返回本地 Top2：
  //   range 0 -> (1, 100.0), (3, 97.0)
  //   range 1 -> (2, 99.0),  (4, 96.0)
  // If we skip the path-local top-k merge before Fusion, docs 3/4 would
  // incorrectly keep Query-path contributions.
  ObDASFusionIter iter;

  ObDASFusionPathSliceWork query_rt0, query_rt1, vec_rt;
  query_rt0.exec_env_.path_idx_ = 0;
  init_docid_range_slice(query_rt0, 0, 0, 2);
  query_rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 100.0));
  query_rt0.rows_.push_back(ObDASFusionMaterializedRow(3, 97.0));

  query_rt1.exec_env_.path_idx_ = 0;
  init_docid_range_slice(query_rt1, 0, 1, 2);
  query_rt1.rows_.push_back(ObDASFusionMaterializedRow(2, 99.0));
  query_rt1.rows_.push_back(ObDASFusionMaterializedRow(4, 96.0));

  vec_rt.exec_env_.path_idx_ = 1;
  vec_rt.rows_.push_back(ObDASFusionMaterializedRow(3, 100.0));
  vec_rt.rows_.push_back(ObDASFusionMaterializedRow(1, 1.0));

  iter.parallel_ctx_.path_slice_works_.push_back(&query_rt0);
  iter.parallel_ctx_.path_slice_works_.push_back(&query_rt1);
  iter.parallel_ctx_.path_slice_works_.push_back(&vec_rt);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 2;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());

  EXPECT_EQ(3, iter.fusion_docs_.count());

  bool found_doc1 = false;
  bool found_doc2 = false;
  bool found_doc3 = false;
  bool found_doc4 = false;
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    const ObDASFusionDocInfo &doc = iter.fusion_docs_.at(i);
    const uint64_t rowkey = doc.get_uint64_rowkey();
    if (1 == rowkey) {
      found_doc1 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
    } else if (2 == rowkey) {
      found_doc2 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_FALSE(doc.has_path(1));
    } else if (3 == rowkey) {
      found_doc3 = true;
      EXPECT_FALSE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
    } else if (4 == rowkey) {
      found_doc4 = true;
    }
  }

  EXPECT_TRUE(found_doc1);
  EXPECT_TRUE(found_doc2);
  EXPECT_TRUE(found_doc3);
  EXPECT_FALSE(found_doc4);

  iter.parallel_ctx_.path_slice_works_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ParallelMergeSimulationTest, simulate_empty_works)
{
  // 所有 Work 均成功完成，但没有结果行。
  ObDASFusionIter iter;

  ObDASFusionPathSliceWork rt0, rt1;
  rt0.exec_env_.path_idx_ = 0;

  rt1.exec_env_.path_idx_ = 1;

  iter.parallel_ctx_.path_slice_works_.push_back(&rt0);
  iter.parallel_ctx_.path_slice_works_.push_back(&rt1);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());
  EXPECT_EQ(0, iter.fusion_docs_.count());

  iter.parallel_ctx_.path_slice_works_.reset();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ParallelMergeSimulationTest, simulate_null_work_in_list)
{
  ObDASFusionIter iter;

  iter.parallel_ctx_.path_slice_works_.push_back(nullptr);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.children_cnt_ = 2;

  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.merge_parallel_results());

  iter.parallel_ctx_.path_slice_works_.reset();
}

// ============================================================================
// Section 20: ObFastBitmap and Iterator basic tests
// ============================================================================

class ObFastBitmapTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObFastBitmapTest, init_and_add)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  EXPECT_EQ(0, bitmap.cardinality());

  ASSERT_EQ(OB_SUCCESS, bitmap.add(0));
  EXPECT_EQ(1, bitmap.cardinality());

  ASSERT_EQ(OB_SUCCESS, bitmap.add(100));
  EXPECT_EQ(2, bitmap.cardinality());

  // Adding the same value again should not increase cardinality
  ASSERT_EQ(OB_SUCCESS, bitmap.add(100));
  EXPECT_EQ(2, bitmap.cardinality());
}

TEST_F(ObFastBitmapTest, init_invalid_arg)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  // Negative max_value_count should fail
  EXPECT_EQ(OB_INVALID_ARGUMENT, bitmap.init(-1));
}

TEST_F(ObFastBitmapTest, add_large_value)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  // Value that spans multiple chunks (each chunk covers 2^16 = 65536 values)
  const uint64_t large_val = 100000;
  ASSERT_EQ(OB_SUCCESS, bitmap.add(large_val));
  EXPECT_EQ(1, bitmap.cardinality());
}

TEST_F(ObFastBitmapTest, add_across_chunk_boundary)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  // Values at the chunk boundary (65535 and 65536)
  const uint64_t last_in_chunk0 = (1ULL << 16) - 1; // 65535
  const uint64_t first_in_chunk1 = (1ULL << 16);     // 65536
  ASSERT_EQ(OB_SUCCESS, bitmap.add(last_in_chunk0));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(first_in_chunk1));
  EXPECT_EQ(2, bitmap.cardinality());
}

TEST_F(ObFastBitmapTest, add_batch)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  ASSERT_EQ(OB_SUCCESS, bitmap.add(65));
  const uint64_t first_batch[] = {
      ObFastBitmap::CHUNK_SIZE + 9, 130, 66, 65, 64, 1, 0, 0};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(first_batch, ARRAYSIZEOF(first_batch)));
  const uint64_t second_batch[] = {
      ObFastBitmap::CHUNK_SIZE + 3, 66, 131, 2, 0, ObFastBitmap::CHUNK_SIZE + 9};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(second_batch, ARRAYSIZEOF(second_batch)));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(132));
  EXPECT_TRUE(bitmap.stats_dirty_);

  uint64_t actual = UINT64_MAX;
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.next_id(actual));
  ObFastBitmap::Iterator dirty_iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, dirty_iter.init(&bitmap));

  bitmap.finalize();
  EXPECT_FALSE(bitmap.stats_dirty_);
  EXPECT_EQ(11, bitmap.cardinality());
  ASSERT_GE(bitmap.chunks_.count(), 2);
  ASSERT_NE(nullptr, bitmap.chunks_.at(0));
  ASSERT_NE(nullptr, bitmap.chunks_.at(1));
  EXPECT_EQ(0, bitmap.chunks_.at(0)->min_val_);
  EXPECT_EQ(132, bitmap.chunks_.at(0)->max_val_);
  EXPECT_EQ(3, bitmap.chunks_.at(1)->min_val_);
  EXPECT_EQ(9, bitmap.chunks_.at(1)->max_val_);

  const uint64_t expected[] = {
      0, 1, 2, 64, 65, 66, 130, 131, 132,
      ObFastBitmap::CHUNK_SIZE + 3, ObFastBitmap::CHUNK_SIZE + 9};
  for (uint64_t value : expected) {
    ASSERT_EQ(OB_SUCCESS, iter.next_id(actual));
    EXPECT_EQ(value, actual);
  }
  EXPECT_EQ(OB_ITER_END, iter.next_id(actual));
}

TEST_F(ObFastBitmapTest, add_batch_large_unsorted)
{
  static const int64_t BATCH_COUNT = 300;
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  uint64_t reversed_values[BATCH_COUNT];
  uint64_t sorted_values[BATCH_COUNT];
  for (int64_t i = 0; i < BATCH_COUNT; ++i) {
    reversed_values[i] = (BATCH_COUNT - i - 1) * 257;
    sorted_values[i] = i * 257;
  }

  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(reversed_values, BATCH_COUNT));
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(sorted_values, BATCH_COUNT));
  EXPECT_EQ((BATCH_COUNT - 1) * 257, reversed_values[0]);
  bitmap.finalize();
  EXPECT_EQ(BATCH_COUNT, bitmap.cardinality());
  bitmap.finalize();
  EXPECT_EQ(BATCH_COUNT, bitmap.cardinality());

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  for (int64_t i = 0; i < BATCH_COUNT; ++i) {
    uint64_t actual = UINT64_MAX;
    ASSERT_EQ(OB_SUCCESS, iter.next_id(actual));
    EXPECT_EQ(sorted_values[i], actual);
  }
  uint64_t actual = UINT64_MAX;
  EXPECT_EQ(OB_ITER_END, iter.next_id(actual));
}

TEST_F(ObFastBitmapTest, add_batch_finalize_chunk_boundaries)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  const uint64_t values[] = {
      0, 63, 64, ObFastBitmap::CHUNK_SIZE - 1,
      ObFastBitmap::CHUNK_SIZE, 2 * ObFastBitmap::CHUNK_SIZE - 1};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values, ARRAYSIZEOF(values)));
  bitmap.finalize();

  EXPECT_EQ(6, bitmap.cardinality());
  ASSERT_GE(bitmap.chunks_.count(), 2);
  ASSERT_NE(nullptr, bitmap.chunks_.at(0));
  ASSERT_NE(nullptr, bitmap.chunks_.at(1));
  EXPECT_EQ(0, bitmap.chunks_.at(0)->min_val_);
  EXPECT_EQ(UINT16_MAX, bitmap.chunks_.at(0)->max_val_);
  EXPECT_EQ(0, bitmap.chunks_.at(1)->min_val_);
  EXPECT_EQ(UINT16_MAX, bitmap.chunks_.at(1)->max_val_);
}

TEST_F(ObFastBitmapTest, add_batch_finalize_skips_sparse_chunk_slots)
{
  static const uint64_t SPARSE_CHUNK_IDX = 64;
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(0));

  const uint64_t chunk_base = SPARSE_CHUNK_IDX << ObFastBitmap::CHUNK_SHIFT;
  const uint64_t values[] = {chunk_base + 100, chunk_base + 7, chunk_base + 100};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values, ARRAYSIZEOF(values)));
  bitmap.finalize();

  EXPECT_EQ(2, bitmap.cardinality());
  ASSERT_EQ(static_cast<int64_t>(SPARSE_CHUNK_IDX + 1), bitmap.chunks_.count());
  EXPECT_EQ(nullptr, bitmap.chunks_.at(0));
  ASSERT_NE(nullptr, bitmap.chunks_.at(SPARSE_CHUNK_IDX));
  EXPECT_EQ(7, bitmap.chunks_.at(SPARSE_CHUNK_IDX)->min_val_);
  EXPECT_EQ(100, bitmap.chunks_.at(SPARSE_CHUNK_IDX)->max_val_);
}

TEST_F(ObFastBitmapTest, add_batch_validates_arguments)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  const uint64_t value = 0;
  EXPECT_EQ(OB_SUCCESS, bitmap.add_batch(&value, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bitmap.add_batch(nullptr, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bitmap.add_batch(nullptr, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bitmap.add_batch(&value, -1));
  EXPECT_EQ(0, bitmap.cardinality());
}

TEST_F(ObFastBitmapTest, add_batch_after_reuse)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  const uint64_t first_batch[] = {1, 2, 3};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(first_batch, ARRAYSIZEOF(first_batch)));
  bitmap.reuse();

  const uint64_t second_batch[] = {ObFastBitmap::CHUNK_SIZE + 1, 5, 5};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(second_batch, ARRAYSIZEOF(second_batch)));
  bitmap.finalize();
  EXPECT_EQ(2, bitmap.cardinality());

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  uint64_t actual = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, iter.next_id(actual));
  EXPECT_EQ(5ULL, actual);
  ASSERT_EQ(OB_SUCCESS, iter.next_id(actual));
  EXPECT_EQ(ObFastBitmap::CHUNK_SIZE + 1, actual);
  EXPECT_EQ(OB_ITER_END, iter.next_id(actual));
}

TEST_F(ObFastBitmapTest, copy_unaligned_range_to_words_across_chunk_boundary)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  const uint64_t chunk_end = ObFastBitmap::CHUNK_SIZE - 1;
  const uint64_t values[] = {
      chunk_end - 6, chunk_end - 2, chunk_end,
      chunk_end + 1, chunk_end + 2, chunk_end + 44,
      chunk_end + 45, chunk_end + 46};
  ASSERT_EQ(OB_SUCCESS, bitmap.init(2 * ObFastBitmap::CHUNK_SIZE));
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values, ARRAYSIZEOF(values)));
  bitmap.finalize();

  const uint64_t window_begin = chunk_end - 5;
  const int64_t capacity = 80;
  uint64_t words[2] = {~0ULL, ~0ULL};
  int64_t copied_count = 0;
  ASSERT_EQ(OB_SUCCESS, bitmap.copy_range_to_words(
      chunk_end - 2, chunk_end + 45, window_begin, capacity, words, copied_count));
  EXPECT_EQ(6, copied_count);
  for (int64_t offset = 0; offset < capacity; ++offset) {
    const uint64_t doc_id = window_begin + offset;
    const bool expected = doc_id == chunk_end - 2
        || doc_id == chunk_end
        || doc_id == chunk_end + 1
        || doc_id == chunk_end + 2
        || doc_id == chunk_end + 44
        || doc_id == chunk_end + 45;
    EXPECT_EQ(expected, 0 != (words[offset >> 6] & (1ULL << (offset & 63))))
        << "doc_id=" << doc_id;
  }
}

TEST_F(ObFastBitmapTest, copy_range_to_words_matches_membership_at_every_word_alignment)
{
  ObArenaAllocator alloc("TestBmpWords");
  ObFastBitmap bitmap(alloc);
  const uint64_t values[] = {0, 1, 31, 63, 64, 65, 95, 127, 128, 191, 255};
  ASSERT_EQ(OB_SUCCESS, bitmap.init(3 * ObFastBitmap::CHUNK_SIZE));
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values, ARRAYSIZEOF(values)));
  bitmap.finalize();
  const int64_t capacities[] = {1, 63, 64, 65, 127};
  for (uint64_t shift = 0; shift < 64; ++shift) {
    for (int64_t capacity : capacities) {
      uint64_t words[2] = {~0ULL, ~0ULL};
      int64_t copied_count = 0;
      int64_t expected_count = 0;
      ASSERT_EQ(OB_SUCCESS, bitmap.copy_range_to_words(
          shift, shift + capacity - 1, shift, capacity, words, copied_count));
      const int64_t rounded_capacity = ((capacity + 63) / 64) * 64;
      for (int64_t bit = 0; bit < rounded_capacity; ++bit) {
        const bool expected = bit < capacity
            && std::find(values, values + ARRAYSIZEOF(values), shift + bit)
                != values + ARRAYSIZEOF(values);
        EXPECT_EQ(expected, 0 != (words[bit / 64] & (1ULL << (bit % 64))));
        expected_count += expected;
      }
      EXPECT_EQ(expected_count, copied_count);
    }
  }
  uint64_t empty_words[2] = {~0ULL, ~0ULL};
  int64_t empty_count = -1;
  ASSERT_EQ(OB_SUCCESS, bitmap.copy_range_to_words(
      ObFastBitmap::CHUNK_SIZE + 1, ObFastBitmap::CHUNK_SIZE + 65,
      ObFastBitmap::CHUNK_SIZE + 1, 65, empty_words, empty_count));
  EXPECT_EQ(0, empty_count);
  EXPECT_EQ(0ULL, empty_words[0]);
  EXPECT_EQ(0ULL, empty_words[1]);
}

TEST_F(ObFastBitmapTest, copy_range_to_words_validates_state_and_arguments)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  const uint64_t value = 10;
  uint64_t words[2] = {0, 0};
  int64_t copied_count = 0;
  ASSERT_EQ(OB_SUCCESS, bitmap.init(128));
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(&value, 1));
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            bitmap.copy_range_to_words(10, 10, 0, 128, words, copied_count));
  bitmap.finalize();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            bitmap.copy_range_to_words(10, 9, 0, 128, words, copied_count));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            bitmap.copy_range_to_words(10, 10, 11, 128, words, copied_count));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            bitmap.copy_range_to_words(10, 10, 0, 10, words, copied_count));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            bitmap.copy_range_to_words(10, 10, 0, 128, nullptr, copied_count));
}

// ============================================================================
// Section 21: ObFastBitmap::Iterator edge cases
// ============================================================================

TEST_F(ObFastBitmapTest, iterator_init_null)
{
  ObFastBitmap::Iterator iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.init(nullptr));
}

TEST_F(ObFastBitmapTest, iterator_not_inited)
{
  ObFastBitmap::Iterator iter;
  EXPECT_FALSE(iter.is_inited());
}

TEST_F(ObFastBitmapTest, iterator_zero_is_first_and_reuse_rewinds)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(0));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(0ULL, val);

  EXPECT_EQ(OB_ITER_END, iter.next_id(val));

  iter.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(0ULL, val);
}

TEST_F(ObFastBitmapTest, iterator_init_and_next)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(10));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(20));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(30));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(10ULL, val);

  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(20ULL, val);

  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(30ULL, val);

  // Past the end
  EXPECT_EQ(OB_ITER_END, iter.next_id(val));
}

TEST_F(ObFastBitmapTest, iterator_advance_to)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(10));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(20));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(30));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(50));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  // Advance to 15 -> should land on 20
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(15, val));
  EXPECT_EQ(20ULL, val);

  // Advance to 20 -> should stay at 20
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(20, val));
  EXPECT_EQ(20ULL, val);

  // Advance to 25 -> should land on 30
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(25, val));
  EXPECT_EQ(30ULL, val);

  // Advance to 40 -> should land on 50
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(40, val));
  EXPECT_EQ(50ULL, val);

  // Advance beyond any value -> ITER_END
  EXPECT_EQ(OB_ITER_END, iter.advance_to(100, val));
}


TEST_F(ObFastBitmapTest, iterator_on_empty_bitmap)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  // No values added

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  EXPECT_EQ(OB_ITER_END, iter.next_id(val));
  EXPECT_EQ(OB_ITER_END, iter.advance_to(0, val));
}

TEST_F(ObFastBitmapTest, iterator_reuse)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(5));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(15));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(5ULL, val);

  iter.reuse();
  // After reuse, should start from the beginning again
  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(5ULL, val);
  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(15ULL, val);
}

TEST_F(ObFastBitmapTest, iterator_reset)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(5));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  EXPECT_TRUE(iter.is_inited());

  iter.reset();
  EXPECT_FALSE(iter.is_inited());
}

TEST_F(ObFastBitmapTest, iterator_advance_to_before_current)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(10));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(30));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  // Move to 30
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(30, val));
  EXPECT_EQ(30ULL, val);

  // Advance to 10 (before current) -> should return current position (30)
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(10, val));
  EXPECT_EQ(30ULL, val);
}

TEST_F(ObFastBitmapTest, iterator_native_drain_validates_arguments_and_empty_bitmap)
{
  uint64_t ids[2] = {};
  int64_t count = 1;
  uint64_t next_id = 1;
  ObFastBitmap::Iterator::DrainState state =
      ObFastBitmap::Iterator::DRAIN_BATCH_FULL;

  ObFastBitmap::Iterator uninitialized_iter;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      uninitialized_iter.drain_until(10, ARRAYSIZEOF(ids), ids,
                                     count, next_id, state));
  EXPECT_EQ(0, count);
  EXPECT_EQ(0ULL, next_id);
  EXPECT_EQ(ObFastBitmap::Iterator::DRAIN_ITER_END, state);

  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  EXPECT_EQ(OB_INVALID_ARGUMENT,
      iter.drain_until(10, 0, ids, count, next_id, state));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      iter.drain_until(10, -1, ids, count, next_id, state));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      iter.drain_until(10, ARRAYSIZEOF(ids), nullptr,
                       count, next_id, state));

  ASSERT_EQ(OB_SUCCESS,
      iter.drain_until(10, ARRAYSIZEOF(ids), ids,
                       count, next_id, state));
  EXPECT_EQ(0, count);
  EXPECT_EQ(0ULL, next_id);
  EXPECT_EQ(ObFastBitmap::Iterator::DRAIN_ITER_END, state);
}

TEST_F(ObFastBitmapTest, iterator_native_drain_starts_after_current)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  const uint64_t values[] = {1, 3, 5, 7};
  for (uint64_t value : values) {
    ASSERT_EQ(OB_SUCCESS, bitmap.add(value));
  }

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  uint64_t current = 0;
  ASSERT_EQ(OB_SUCCESS, iter.advance_to(3, current));
  ASSERT_EQ(3ULL, current);

  uint64_t ids[4] = {};
  int64_t count = 0;
  uint64_t next_id = 0;
  ObFastBitmap::Iterator::DrainState state =
      ObFastBitmap::Iterator::DRAIN_ITER_END;
  ASSERT_EQ(OB_SUCCESS,
      iter.drain_until(5, ARRAYSIZEOF(ids), ids, count, next_id, state));
  ASSERT_EQ(1, count);
  EXPECT_EQ(5ULL, ids[0]);
  EXPECT_EQ(7ULL, next_id);
  EXPECT_EQ(ObFastBitmap::Iterator::DRAIN_BOUNDARY_REACHED, state);
}

TEST_F(ObFastBitmapTest, iterator_native_drain_across_words_and_chunks)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  const uint64_t values[] = {0, 1, 2, 63, 64, 65, 65535, 65536, 65537, 131079};
  for (uint64_t value : values) {
    ASSERT_EQ(OB_SUCCESS, bitmap.add(value));
  }

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  uint64_t ids[16] = {};
  int64_t count = 0;
  uint64_t next_id = 0;
  ObFastBitmap::Iterator::DrainState state =
      ObFastBitmap::Iterator::DRAIN_ITER_END;
  ASSERT_EQ(OB_SUCCESS,
      iter.drain_until(65, ARRAYSIZEOF(ids), ids, count, next_id, state));
  ASSERT_EQ(6, count);
  EXPECT_EQ(0ULL, ids[0]);
  EXPECT_EQ(1ULL, ids[1]);
  EXPECT_EQ(2ULL, ids[2]);
  EXPECT_EQ(63ULL, ids[3]);
  EXPECT_EQ(64ULL, ids[4]);
  EXPECT_EQ(65ULL, ids[5]);
  EXPECT_EQ(ObFastBitmap::Iterator::DRAIN_BOUNDARY_REACHED, state);
  EXPECT_EQ(65535ULL, next_id);

  // The boundary lookahead is the iterator's current row and is owned by the
  // caller. Direct row iteration therefore starts after it.
  uint64_t val = 0;
  ASSERT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(65536ULL, val);
}

TEST_F(ObFastBitmapTest, iterator_native_drain_full_batch_and_eof)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  const uint64_t values[] = {0, 1, 2, 3, 64, 65, 131079};
  for (uint64_t value : values) {
    ASSERT_EQ(OB_SUCCESS, bitmap.add(value));
  }

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  uint64_t ids[4] = {};
  int64_t count = 0;
  uint64_t next_id = 0;
  ObFastBitmap::Iterator::DrainState state =
      ObFastBitmap::Iterator::DRAIN_ITER_END;
  ASSERT_EQ(OB_SUCCESS,
      iter.drain_until(UINT64_MAX, ARRAYSIZEOF(ids), ids, count, next_id, state));
  ASSERT_EQ(4, count);
  EXPECT_EQ(0ULL, ids[0]);
  EXPECT_EQ(1ULL, ids[1]);
  EXPECT_EQ(2ULL, ids[2]);
  EXPECT_EQ(3ULL, ids[3]);
  EXPECT_EQ(ObFastBitmap::Iterator::DRAIN_BATCH_FULL, state);

  ASSERT_EQ(OB_SUCCESS,
      iter.drain_until(UINT64_MAX, ARRAYSIZEOF(ids), ids, count, next_id, state));
  ASSERT_EQ(3, count);
  EXPECT_EQ(64ULL, ids[0]);
  EXPECT_EQ(65ULL, ids[1]);
  EXPECT_EQ(131079ULL, ids[2]);
  EXPECT_EQ(ObFastBitmap::Iterator::DRAIN_ITER_END, state);

  ASSERT_EQ(OB_SUCCESS,
      iter.drain_until(UINT64_MAX, ARRAYSIZEOF(ids), ids, count, next_id, state));
  EXPECT_EQ(0, count);
  EXPECT_EQ(ObFastBitmap::Iterator::DRAIN_ITER_END, state);
}

// ============================================================================
// Section 23: ObDASFusionParallelCoordinator additional tests
// ============================================================================

TEST_F(ObDASFusionParallelCoordinatorTest, double_init)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));
  EXPECT_TRUE(coord.is_inited());
  EXPECT_EQ(3, coord.total_cnt_);

  // Second init should update count
  ASSERT_EQ(OB_SUCCESS, coord.init(5));
  EXPECT_EQ(5, coord.total_cnt_);
  EXPECT_EQ(0, coord.finished_cnt_);
}

TEST_F(ObDASFusionParallelCoordinatorTest, wait_already_complete)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(2));
  coord.on_work_unit_finish(OB_SUCCESS);
  coord.on_work_unit_finish(OB_SUCCESS);

  // Already complete, wait should return immediately
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));

  // Calling wait again should also return immediately
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, set_first_error_ignores_success_detailed)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  // Setting OB_SUCCESS should not change anything
  coord.set_first_error(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, coord.get_first_error());

  // Setting an actual error should update
  coord.set_first_error(OB_ERR_UNEXPECTED);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());

  // A second error should not overwrite the first
  coord.set_first_error(OB_TIMEOUT);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());

  // OB_SUCCESS should not overwrite the error
  coord.set_first_error(OB_SUCCESS);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());
}

// ============================================================================
// Section 24: ObSharedBitmapSlot additional tests
// ============================================================================

TEST(ObSharedBitmapSlotAdditionalTest, occurrence_index_default_and_reset)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  EXPECT_EQ(-1, slot.bitmap_occurrence_idx_);
  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);

  slot.bitmap_occurrence_idx_ = 3;
  slot.is_built_ = true;
  slot.build_ret_ = OB_ERR_UNEXPECTED;

  slot.release();

  // After release, occurrence idx is NOT reset (only bitmap_, is_built_, build_ret_ are reset)
  // The release() function only resets bitmap-related state
  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

// ============================================================================
// 第 25 节：ObDASFusionPathSliceWork 结果物化空指针保护
// ============================================================================

TEST_F(ObDASFusionTaskExecEnvTest, materialize_batch_result_null_eval_ctx)
{
  ObDASFusionPathSliceWork work;
  work.exec_env_.child_eval_ctx_ = nullptr;
  work.rowkey_expr_ =
      reinterpret_cast<ObExpr *>(0xDEAD);
  work.exec_env_.score_expr_ =
      reinterpret_cast<ObExpr *>(0xBEEF);

  EXPECT_EQ(OB_ERR_UNEXPECTED, work.materialize_batch_result(1));
}

TEST_F(ObDASFusionTaskExecEnvTest, materialize_batch_result_null_rowkey_expr)
{
  ObDASFusionPathSliceWork work;
  ObExecContext exec_ctx(work.exec_env_.child_allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  work.exec_env_.child_eval_ctx_ = &eval_ctx;
  work.rowkey_expr_ = nullptr;
  work.exec_env_.score_expr_ =
      reinterpret_cast<ObExpr *>(0xBEEF);

  EXPECT_EQ(OB_ERR_UNEXPECTED, work.materialize_batch_result(1));
}

TEST_F(ObDASFusionTaskExecEnvTest, materialize_batch_result_null_score_expr)
{
  ObDASFusionPathSliceWork work;
  ObExecContext exec_ctx(work.exec_env_.child_allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  work.exec_env_.child_eval_ctx_ = &eval_ctx;
  work.rowkey_expr_ =
      reinterpret_cast<ObExpr *>(0xDEAD);
  work.exec_env_.score_expr_ = nullptr;

  EXPECT_EQ(OB_ERR_UNEXPECTED, work.materialize_batch_result(1));
}

TEST_F(ObDASFusionTaskExecEnvTest, materialize_batch_result_zero_batch_size)
{
  ObDASFusionPathSliceWork work;
  ObExecContext exec_ctx(work.exec_env_.child_allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  work.exec_env_.child_eval_ctx_ = &eval_ctx;
  work.rowkey_expr_ =
      reinterpret_cast<ObExpr *>(0xDEAD);
  work.exec_env_.score_expr_ =
      reinterpret_cast<ObExpr *>(0xBEEF);

  // batch_size <= 0 should fail
  EXPECT_EQ(OB_ERR_UNEXPECTED, work.materialize_batch_result(0));
  EXPECT_EQ(OB_ERR_UNEXPECTED, work.materialize_batch_result(-1));
}

TEST_F(ObDASFusionTaskExecEnvTest, materialize_batch_result_negative_batch_size)
{
  ObDASFusionPathSliceWork work;
  ObExecContext exec_ctx(work.exec_env_.child_allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  work.exec_env_.child_eval_ctx_ = &eval_ctx;
  work.rowkey_expr_ =
      reinterpret_cast<ObExpr *>(0xDEAD);
  work.exec_env_.score_expr_ =
      reinterpret_cast<ObExpr *>(0xBEEF);

  EXPECT_EQ(OB_ERR_UNEXPECTED, work.materialize_batch_result(-5));
}

// ============================================================================
// 第 26 节：ObDASFusionPathSliceWork 排空迭代器空指针保护
// ============================================================================

TEST_F(ObDASFusionTaskExecEnvTest, drain_child_iter_null_iter)
{
  ObDASFusionPathSliceWork work;
  work.exec_env_.child_iter_ = nullptr;

  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  EXPECT_EQ(OB_ERR_UNEXPECTED, work.drain_child_iter(coord));
}

// ============================================================================
// 第 27 节：ObDASFusionPathSliceWork Bitmap 注入校验
// ============================================================================

TEST_F(ObDASFusionTaskExecEnvTest, inject_shared_bitmaps_null_parallel_ctx)
{
  ObDASFusionPathSliceWork work;
  work.parallel_ctx_ = nullptr;

  EXPECT_EQ(OB_ERR_UNEXPECTED, work.inject_shared_bitmaps());
}

TEST_F(ObDASFusionTaskExecEnvTest, inject_shared_bitmaps_no_shared_bitmaps)
{
  ObDASFusionPathSliceWork work;
  ObDASFusionParallelCtx ctx;
  work.parallel_ctx_ = &ctx;
  EXPECT_FALSE(ctx.has_shared_bitmaps());

  EXPECT_EQ(OB_SUCCESS, work.inject_shared_bitmaps());
}

TEST_F(ObDASFusionTaskExecEnvTest, inject_shared_bitmaps_null_bitmap_in_slot)
{
  // PREPARE 完成后共享 Bitmap Slot 仍为空时，注入应当失败。
  ObDASFusionPathSliceWork work;
  ObArenaAllocator alloc("TestInj");
  ObDASFusionParallelCtx ctx;
  work.parallel_ctx_ = &ctx;

  // Manually push a slot with null bitmap
  ObSharedBitmapSlot *slot = nullptr;
  void *slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot));
  ASSERT_NE(nullptr, slot_buf);
  slot = new (slot_buf) ObSharedBitmapSlot();
  slot->bitmap_occurrence_idx_ = 0;
  slot->bitmap_ = nullptr;  // null bitmap!
  ATOMIC_STORE(&slot->is_built_, true);

  ctx.shared_bitmap_slots_.push_back(slot);

  // child_bitmap_ops_ is empty, so bitmap_occurrence_idx_ check should also fail
  // But first comes the null bitmap check
  EXPECT_EQ(OB_ERR_UNEXPECTED, work.inject_shared_bitmaps());

  // Clean up
  slot->release();
  slot->~ObSharedBitmapSlot();
}

// ============================================================================
// 第 28 节：ObDASFusionBitmapPrepareWork 校验
// ============================================================================

TEST_F(ObDASFusionTaskExecEnvTest, build_shared_bitmaps_null_parallel_ctx)
{
  ObDASFusionBitmapPrepareWork work;
  ObDASFusionParallelCoordinator coordinator;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            work.build_shared_bitmaps(INT64_MAX, coordinator));
}

TEST_F(ObDASFusionTaskExecEnvTest, path_slice_work_owns_slice_payload)
{
  ObDASFusionPathSliceWork work;
  ASSERT_EQ(OB_SUCCESS,
            work.work_slice_.init_whole_path(0));
  EXPECT_TRUE(work.work_slice_.is_valid());
  EXPECT_EQ(0, work.work_slice_.path_id_);
}

TEST_F(ObDASFusionTaskExecEnvTest, build_shared_bitmaps_empty_prepare)
{
  ObDASFusionBitmapPrepareWork work;
  ObDASFusionParallelCtx ctx;
  ObDASFusionParallelCoordinator coordinator;
  work.parallel_ctx_ = &ctx;
  EXPECT_EQ(OB_SUCCESS,
            work.build_shared_bitmaps(INT64_MAX, coordinator));
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_failure_partial_drain_stops_other_views)
{
  ObArenaAllocator alloc("ProviderFail");
  ObArenaAllocator scan_alloc("ProviderScan");
  ObFastBitmap external_bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, external_bitmap.init(8));
  ASSERT_EQ(OB_SUCCESS, external_bitmap.add(8));
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  eval_ctx.max_batch_size_ = 4;
  search_ctx.eval_ctx_ = &eval_ctx;
  TestFilterProviderFailureSourceParam source_param(false, 6);
  TestFilterProviderFailureSourceOp *source = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_op(source_param, source));
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      source, provider));
  ASSERT_NE(nullptr, provider);
  ObDASBitmapOp *first_view = nullptr;
  ObDASBitmapOp *second_view = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider->create_view(search_ctx, first_view));
  ASSERT_EQ(OB_SUCCESS, provider->create_view(search_ctx, second_view));
  ASSERT_EQ(OB_SUCCESS, first_view->open());
  ASSERT_EQ(OB_SUCCESS, second_view->open());
  ObDASRowID rowid;
  double score = 0.0;
  // [1,2,3,4] is stored; [5,6] is consumed by the failing second drain batch.
  ASSERT_EQ(OB_TIMEOUT, first_view->next_rowid(rowid, score));
  ASSERT_EQ(6, source->get_consumed_count());
  ASSERT_EQ(7, source->get_next_count());
  EXPECT_FALSE(provider->is_prepared());
  EXPECT_EQ(nullptr, provider->get_bitmap());
  EXPECT_EQ(OB_TIMEOUT, second_view->next_rowid(rowid, score));
  EXPECT_EQ(OB_TIMEOUT, provider->ensure_prepared());
  EXPECT_EQ(OB_TIMEOUT, provider->prepare(false));
  EXPECT_EQ(OB_TIMEOUT, provider->prepare(true));
  EXPECT_EQ(OB_TIMEOUT, provider->reset_for_rescan());
  EXPECT_EQ(OB_TIMEOUT, provider->set_external_bitmap(&external_bitmap));
  EXPECT_FALSE(provider->is_prepared());
  EXPECT_EQ(nullptr, provider->get_bitmap());
  EXPECT_EQ(1, source->get_open_count());
  EXPECT_EQ(0, source->get_rescan_count());
  EXPECT_EQ(7, source->get_next_count());
  EXPECT_EQ(6, source->get_consumed_count());
  // Close the Views while their Provider is still alive.
  search_ctx.reset();
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_failure_partial_open_is_not_reopened)
{
  ObArenaAllocator alloc("ProviderOpen");
  ObArenaAllocator scan_alloc("ProviderScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  eval_ctx.max_batch_size_ = 4;
  search_ctx.eval_ctx_ = &eval_ctx;
  TestFilterProviderFailureSourceParam source_param(true, -1);
  TestFilterProviderFailureSourceOp *source = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_op(source_param, source));
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      source, provider));
  ASSERT_NE(nullptr, provider);
  ASSERT_EQ(OB_TIMEOUT, provider->ensure_prepared());
  EXPECT_TRUE(source->has_opened_resource());
  EXPECT_EQ(OB_TIMEOUT, provider->ensure_prepared());
  EXPECT_EQ(OB_TIMEOUT, provider->prepare(false));
  EXPECT_EQ(OB_TIMEOUT, provider->prepare(true));
  EXPECT_EQ(OB_TIMEOUT, provider->reset_for_rescan());
  EXPECT_EQ(1, source->get_open_count());
  EXPECT_EQ(0, source->get_rescan_count());
  EXPECT_EQ(0, source->get_next_count());
  EXPECT_FALSE(provider->is_prepared());
  EXPECT_EQ(nullptr, provider->get_bitmap());
  search_ctx.reset();
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_failure_shared_build_is_not_restarted)
{
  ObArenaAllocator alloc("ProviderShare");
  ObArenaAllocator scan_alloc("ProviderScan");
  ObArenaAllocator bitmap_alloc("ProviderSlot");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  eval_ctx.max_batch_size_ = 4;
  search_ctx.eval_ctx_ = &eval_ctx;
  TestFilterProviderFailureSourceParam source_param(false, 6);
  TestFilterProviderFailureSourceOp *source = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_op(source_param, source));
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      source, provider));
  ASSERT_NE(nullptr, provider);
  ObFastBitmap *first_bitmap = nullptr;
  ObFastBitmap *second_bitmap = nullptr;
  ASSERT_EQ(OB_TIMEOUT, provider->build_shared_bitmap(bitmap_alloc, first_bitmap));
  EXPECT_EQ(nullptr, first_bitmap);
  ASSERT_EQ(6, source->get_consumed_count());
  EXPECT_EQ(OB_TIMEOUT, provider->build_shared_bitmap(bitmap_alloc, second_bitmap));
  EXPECT_EQ(nullptr, second_bitmap);
  EXPECT_EQ(OB_TIMEOUT, provider->ensure_prepared());
  EXPECT_EQ(OB_TIMEOUT, provider->reset_for_rescan());
  EXPECT_FALSE(provider->is_prepared());
  EXPECT_EQ(nullptr, provider->get_bitmap());
  EXPECT_EQ(1, source->get_open_count());
  EXPECT_EQ(0, source->get_rescan_count());
  EXPECT_EQ(7, source->get_next_count());
  search_ctx.reset();
  // On the pre-fix implementation the unexpected retry publishes a bitmap.
  // Keep the negative-control test's cleanup valid as well.
  if (OB_NOT_NULL(second_bitmap)) {
    second_bitmap->~ObFastBitmap();
    bitmap_alloc.free(second_bitmap);
  }
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_failure_rescan_hides_previously_prepared_bitmap)
{
  ObArenaAllocator alloc("ProviderScan");
  ObArenaAllocator scan_alloc("ProviderInput");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  eval_ctx.max_batch_size_ = 4;
  search_ctx.eval_ctx_ = &eval_ctx;
  TestFilterProviderFailureSourceParam source_param(false, -1, true);
  TestFilterProviderFailureSourceOp *source = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_op(source_param, source));
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      source, provider));
  ASSERT_NE(nullptr, provider);
  ASSERT_EQ(OB_SUCCESS, provider->ensure_prepared());
  ASSERT_NE(nullptr, provider->get_bitmap());
  ASSERT_EQ(8, provider->get_bitmap()->cardinality());
  // Exercise prepare(true) itself, without relying on Driver reset to hide
  // the old snapshot before Source::rescan fails.
  ASSERT_EQ(OB_TIMEOUT, provider->prepare(true));
  EXPECT_FALSE(provider->is_prepared());
  EXPECT_EQ(nullptr, provider->get_bitmap());
  EXPECT_EQ(OB_TIMEOUT, provider->ensure_prepared());
  EXPECT_EQ(OB_TIMEOUT, provider->prepare(true));
  EXPECT_EQ(OB_TIMEOUT, provider->reset_for_rescan());
  EXPECT_EQ(1, source->get_open_count());
  EXPECT_EQ(1, source->get_rescan_count());
  EXPECT_EQ(9, source->get_next_count());
  search_ctx.reset();
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_success_survives_rejected_calls_and_rescan)
{
  ObArenaAllocator alloc("ProviderValid");
  ObArenaAllocator scan_alloc("ProviderScan");
  ObArenaAllocator bitmap_alloc("ProviderSlot");
  ObFastBitmap external_bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, external_bitmap.init(8));
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  eval_ctx.max_batch_size_ = 4;
  search_ctx.eval_ctx_ = &eval_ctx;
  TestFilterProviderFailureSourceParam source_param(false, -1);
  TestFilterProviderFailureSourceOp *source = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_op(source_param, source));
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      source, provider));
  ASSERT_NE(nullptr, provider);
  ASSERT_EQ(OB_SUCCESS, provider->ensure_prepared());
  ObFastBitmap *valid_bitmap = provider->get_bitmap();
  ASSERT_NE(nullptr, valid_bitmap);
  EXPECT_EQ(8, valid_bitmap->cardinality());
  ObFastBitmap *shared_bitmap = nullptr;
  EXPECT_EQ(OB_STATE_NOT_MATCH, provider->build_shared_bitmap(bitmap_alloc, shared_bitmap));
  EXPECT_EQ(nullptr, shared_bitmap);
  EXPECT_EQ(OB_INVALID_ARGUMENT, provider->set_external_bitmap(nullptr));
  EXPECT_EQ(OB_STATE_NOT_MATCH, provider->set_external_bitmap(&external_bitmap));
  EXPECT_EQ(OB_SUCCESS, provider->ensure_prepared());
  EXPECT_EQ(valid_bitmap, provider->get_bitmap());
  EXPECT_EQ(1, source->get_open_count());
  EXPECT_EQ(9, source->get_next_count());
  ASSERT_EQ(OB_SUCCESS, provider->reset_for_rescan());
  EXPECT_FALSE(provider->is_prepared());
  ASSERT_EQ(OB_SUCCESS, provider->ensure_prepared());
  ASSERT_NE(nullptr, provider->get_bitmap());
  EXPECT_EQ(8, provider->get_bitmap()->cardinality());
  EXPECT_EQ(1, source->get_open_count());
  EXPECT_EQ(1, source->get_rescan_count());
  search_ctx.reset();
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_external_bitmap_stays_borrowed_on_rescan)
{
  ObArenaAllocator alloc("ProviderBorrow");
  ObArenaAllocator scan_alloc("ProviderScan");
  ObFastBitmap external_bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, external_bitmap.init(8));
  ASSERT_EQ(OB_SUCCESS, external_bitmap.add(2));
  ASSERT_EQ(OB_SUCCESS, external_bitmap.add(6));
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  eval_ctx.max_batch_size_ = 4;
  search_ctx.eval_ctx_ = &eval_ctx;
  TestFilterProviderFailureSourceParam source_param(true, -1);
  TestFilterProviderFailureSourceOp *source = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_op(source_param, source));
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      source, provider));
  ASSERT_NE(nullptr, provider);
  // Rejecting an invalid transition does not start or poison materialization.
  EXPECT_EQ(OB_STATE_NOT_MATCH, provider->prepare(true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, provider->set_external_bitmap(nullptr));
  ASSERT_EQ(OB_SUCCESS, provider->set_external_bitmap(&external_bitmap));
  EXPECT_EQ(OB_SUCCESS, provider->set_external_bitmap(&external_bitmap));
  EXPECT_EQ(OB_SUCCESS, provider->prepare(true));
  EXPECT_EQ(OB_SUCCESS, provider->reset_for_rescan());
  EXPECT_EQ(OB_SUCCESS, provider->ensure_prepared());
  EXPECT_EQ(&external_bitmap, provider->get_bitmap());
  EXPECT_EQ(2, external_bitmap.cardinality());
  EXPECT_EQ(0, source->get_open_count());
  EXPECT_EQ(0, source->get_rescan_count());
  EXPECT_EQ(0, source->get_next_count());
  search_ctx.reset();
  EXPECT_EQ(2, external_bitmap.cardinality());
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_shared_bitmap_build_and_inject)
{
  ObArenaAllocator alloc("ProviderShare");
  ObArenaAllocator scan_alloc("ProviderScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  TestEnumerableSearchOp source_op(
      search_ctx, std::vector<uint64_t>{2, 7, 11});
  ObDASFilterBitmapProvider *provider = nullptr;
  ObDASFusionParallelCtx parallel_ctx;
  ObDASFusionBitmapPrepareWork prepare_work;
  ObDASFusionPathSliceWork slice_work;
  ObDASFusionParallelCoordinator coordinator;
  const ObIDASSearchRtDef *owner =
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234);

  eval_ctx.max_batch_size_ = 8;
  search_ctx.eval_ctx_ = &eval_ctx;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 0, &source_op, provider));
  ASSERT_NE(nullptr, provider);

  void *slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot));
  ASSERT_NE(nullptr, slot_buf);
  ObSharedBitmapSlot *slot = new (slot_buf) ObSharedBitmapSlot();
  slot->filter_provider_idx_ = 0;
  ASSERT_EQ(OB_SUCCESS, parallel_ctx.shared_bitmap_slots_.push_back(slot));
  ASSERT_EQ(OB_SUCCESS, coordinator.init(1));

  prepare_work.parallel_ctx_ = &parallel_ctx;
  prepare_work.exec_env_.child_search_ctx_ = &search_ctx;
  ASSERT_EQ(OB_SUCCESS,
            prepare_work.build_shared_bitmaps(INT64_MAX, coordinator));
  ASSERT_TRUE(ATOMIC_LOAD(&slot->is_built_));
  ASSERT_EQ(OB_SUCCESS, ATOMIC_LOAD(&slot->build_ret_));
  ASSERT_NE(nullptr, slot->bitmap_);
  EXPECT_EQ(3, slot->bitmap_->cardinality());
  EXPECT_TRUE(provider->is_prepared());
  EXPECT_EQ(slot->bitmap_, provider->get_bitmap());

  slice_work.parallel_ctx_ = &parallel_ctx;
  slice_work.exec_env_.child_search_ctx_ = &search_ctx;
  ASSERT_EQ(OB_SUCCESS, slice_work.inject_shared_bitmaps());
  EXPECT_TRUE(provider->is_prepared());
  EXPECT_EQ(slot->bitmap_, provider->get_bitmap());

  search_ctx.set_async_binding(9001, 0, false);
  ObDASBitmapOp *view = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider->create_view(search_ctx, view));
  ASSERT_NE(nullptr, view);
  ASSERT_EQ(OB_SUCCESS, view->open());
  const uint64_t expected[] = {2, 7, 11};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected); ++i) {
    ObDASRowID rowid;
    double score = -1.0;
    ASSERT_EQ(OB_SUCCESS, view->next_rowid(rowid, score));
    EXPECT_EQ(expected[i], rowid.get_uint64());
    EXPECT_DOUBLE_EQ(0.0, score);
  }
  ObDASRowID end_rowid;
  double end_score = 0.0;
  EXPECT_EQ(OB_ITER_END, view->next_rowid(end_rowid, end_score));

  parallel_ctx.release();
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_view_drains_directly_into_unaligned_bitset_windows)
{
  ObArenaAllocator alloc("ProviderDirect");
  ObArenaAllocator scan_alloc("ProviderScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  TestEnumerableSearchOp source_op(
      search_ctx, std::vector<uint64_t>{2, 7, 11});
  ObDASFilterBitmapProvider *provider = nullptr;
  ObDASBitmapOp *view = nullptr;

  eval_ctx.max_batch_size_ = 8;
  search_ctx.eval_ctx_ = &eval_ctx;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      &source_op, provider));
  ASSERT_NE(nullptr, provider);
  ASSERT_EQ(OB_SUCCESS, provider->create_view(search_ctx, view));
  ASSERT_NE(nullptr, view);
  ASSERT_EQ(OB_SUCCESS, view->open());

  ObDASRowID curr_id;
  double score = -1.0;
  ASSERT_EQ(OB_SUCCESS, view->next_rowid(curr_id, score));
  ASSERT_EQ(2, curr_id.get_uint64());
  bool iter_end = false;
  uint64_t filter_words[1] = {~0ULL};
  ObDASRowID batch_start;
  ObDASRowID batch_end;
  batch_start.set_uint64(1);
  batch_end.set_uint64(8);
  ASSERT_EQ(OB_SUCCESS, view->drain_into_filter_bitset(
      batch_start, batch_end, 8, filter_words, curr_id, iter_end));
  EXPECT_EQ((1ULL << 1) | (1ULL << 6), filter_words[0]);
  EXPECT_FALSE(iter_end);
  EXPECT_EQ(11, curr_id.get_uint64());

  filter_words[0] = ~0ULL;
  batch_start.set_uint64(9);
  batch_end.set_uint64(16);
  ASSERT_EQ(OB_SUCCESS, view->drain_into_filter_bitset(
      batch_start, batch_end, 8, filter_words, curr_id, iter_end));
  EXPECT_EQ(1ULL << 2, filter_words[0]);
  EXPECT_TRUE(iter_end);
  search_ctx.reset();
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_word_windows_keep_independent_views_and_docid_range)
{
  ObArenaAllocator alloc("ProviderRange");
  ObArenaAllocator scan_alloc("ProviderScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  TestEnumerableSearchOp source_op(search_ctx, std::vector<uint64_t>{2, 3, 7, 11});
  ObDASFilterBitmapProvider *provider = nullptr;
  ObDASBitmapOp *first_view = nullptr;
  ObDASBitmapOp *second_view = nullptr;
  eval_ctx.max_batch_size_ = 8;
  search_ctx.eval_ctx_ = &eval_ctx;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      &source_op, provider));
  ASSERT_NE(nullptr, provider);
  ASSERT_EQ(OB_SUCCESS, provider->ensure_prepared());
  ASSERT_NE(nullptr, provider->get_bitmap());
  ASSERT_EQ(4, provider->get_bitmap()->cardinality());
  ObObj range_lo;
  ObObj range_hi;
  range_lo.set_uint64(3);
  range_hi.set_uint64(7);
  search_ctx.set_docid_range(range_lo, range_hi);
  ASSERT_EQ(OB_SUCCESS, provider->create_view(search_ctx, first_view));
  ASSERT_EQ(OB_SUCCESS, provider->create_view(search_ctx, second_view));
  ASSERT_EQ(OB_SUCCESS, first_view->open());
  ASSERT_EQ(OB_SUCCESS, second_view->open());
  ObDASRowID first_id;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, first_view->next_rowid(first_id, score));
  EXPECT_EQ(3, first_id.get_uint64());
  ObDASRowID batch_start;
  ObDASRowID batch_end;
  batch_start.set_uint64(1);
  batch_end.set_uint64(8);
  uint64_t words[1] = {~0ULL};
  bool iter_end = false;
  ASSERT_EQ(OB_SUCCESS, first_view->drain_into_filter_bitset(
      batch_start, batch_end, 8, words, first_id, iter_end));
  EXPECT_EQ((1ULL << 2) | (1ULL << 6), words[0]);
  EXPECT_TRUE(iter_end);
  EXPECT_EQ(OB_ITER_END, first_view->next_rowid(first_id, score));
  ObDASRowID second_id;
  ASSERT_EQ(OB_SUCCESS, second_view->next_rowid(second_id, score));
  EXPECT_EQ(3, second_id.get_uint64());
  ASSERT_EQ(OB_SUCCESS, second_view->next_rowid(second_id, score));
  EXPECT_EQ(7, second_id.get_uint64());
  EXPECT_EQ(OB_ITER_END, second_view->next_rowid(second_id, score));
  EXPECT_EQ(4, provider->get_bitmap()->cardinality());
  search_ctx.reset();
}

TEST_F(ObDASFusionTaskExecEnvTest,
       filter_provider_prepare_builds_dependencies_in_registration_order)
{
  ObArenaAllocator alloc("ProviderDeps");
  ObArenaAllocator scan_alloc("ProviderDScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  TestEnumerableSearchOp source_op(
      search_ctx, std::vector<uint64_t>{3, 6, 12});
  ObDASFilterBitmapProvider *query_provider = nullptr;
  ObDASFilterBitmapProvider *group_provider = nullptr;
  ObDASFusionParallelCtx parallel_ctx;
  ObDASFusionBitmapPrepareWork prepare_work;
  ObDASFusionParallelCoordinator coordinator;
  const ObIDASSearchRtDef *owner =
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234);

  eval_ctx.max_batch_size_ = 8;
  search_ctx.eval_ctx_ = &eval_ctx;
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_QUERY_SCOPE,
      DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT, &source_op, query_provider));
  ASSERT_NE(nullptr, query_provider);
  search_ctx.set_async_binding(OB_INVALID_ID, 0, false);
  ObDASBitmapOp *query_view = nullptr;
  ASSERT_EQ(OB_SUCCESS, query_provider->create_view(search_ctx, query_view));
  ASSERT_NE(nullptr, query_view);
  ASSERT_EQ(OB_SUCCESS, search_ctx.create_filter_bitmap_provider(
      owner, DAS_FILTER_BITMAP_GROUP_SCOPE, 0, query_view, group_provider));
  ASSERT_NE(nullptr, group_provider);

  for (int64_t i = 0; i < 2; ++i) {
    void *slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot));
    ASSERT_NE(nullptr, slot_buf);
    ObSharedBitmapSlot *slot = new (slot_buf) ObSharedBitmapSlot();
    slot->filter_provider_idx_ = i;
    ASSERT_EQ(OB_SUCCESS, parallel_ctx.shared_bitmap_slots_.push_back(slot));
  }
  ASSERT_EQ(OB_SUCCESS, coordinator.init(1));
  prepare_work.parallel_ctx_ = &parallel_ctx;
  prepare_work.exec_env_.child_search_ctx_ = &search_ctx;
  ASSERT_EQ(OB_SUCCESS,
            prepare_work.build_shared_bitmaps(INT64_MAX, coordinator));

  ASSERT_TRUE(query_provider->is_prepared());
  ASSERT_TRUE(group_provider->is_prepared());
  for (int64_t i = 0; i < 2; ++i) {
    ObSharedBitmapSlot *slot = parallel_ctx.get_shared_bitmap_slot(i);
    ASSERT_NE(nullptr, slot);
    EXPECT_TRUE(ATOMIC_LOAD(&slot->is_built_));
    EXPECT_EQ(OB_SUCCESS, ATOMIC_LOAD(&slot->build_ret_));
    ASSERT_NE(nullptr, slot->bitmap_);
    EXPECT_EQ(3, slot->bitmap_->cardinality());
  }
  EXPECT_EQ(parallel_ctx.get_shared_bitmap_slot(0)->bitmap_,
            query_provider->get_bitmap());
  EXPECT_EQ(parallel_ctx.get_shared_bitmap_slot(1)->bitmap_,
            group_provider->get_bitmap());
  parallel_ctx.release();
}

TEST_F(ObDASFusionTaskExecEnvTest,
       build_shared_bitmaps_stops_after_other_work_failed)
{
  ObArenaAllocator alloc("PrepareCancel");
  ObDASFusionBitmapPrepareWork work;
  ObDASFusionParallelCtx ctx;
  ObDASFusionParallelCoordinator coordinator;
  void *slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot));
  ASSERT_NE(nullptr, slot_buf);
  ObSharedBitmapSlot *slot = new (slot_buf) ObSharedBitmapSlot();
  slot->bitmap_occurrence_idx_ = 0;
  ASSERT_EQ(OB_SUCCESS, ctx.shared_bitmap_slots_.push_back(slot));
  work.parallel_ctx_ = &ctx;
  coordinator.set_first_error(OB_ERR_UNEXPECTED);

  EXPECT_EQ(OB_CANCELED,
            work.build_shared_bitmaps(INT64_MAX, coordinator));
  EXPECT_TRUE(ATOMIC_LOAD(&slot->is_built_));
  EXPECT_EQ(OB_CANCELED, ATOMIC_LOAD(&slot->build_ret_));
  slot->release();
  slot->~ObSharedBitmapSlot();
}

// ============================================================================
// 第 30 节：ObDASFusionTaskExecEnv 创建并行迭代器的空上下文保护
// ============================================================================

TEST_F(ObDASFusionTaskExecEnvTest, create_parallel_iter_null_contexts)
{
  ObDASFusionTaskExecEnv runtime;
  runtime.path_idx_ = 0;
  // All contexts are null by default
  runtime.child_exec_ctx_ = nullptr;
  runtime.child_eval_ctx_ = nullptr;
  runtime.child_search_ctx_ = nullptr;

  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.create_parallel_iter(-1));
}

// ============================================================================
// 第 31 节：ObDASFusionTaskExecEnv 资源准备的空输入保护
// ============================================================================

TEST_F(ObDASFusionTaskExecEnvTest, prepare_resources_null_src_search_ctx)
{
  ObDASFusionTaskExecEnv runtime;
  ObArenaAllocator alloc("TestRT");
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);

  runtime.src_exec_ctx_ = &exec_ctx;
  runtime.src_eval_ctx_ = &eval_ctx;
  runtime.src_search_ctx_ = nullptr;  // null search ctx

  // Should fail because src_search_ctx_ is null
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            runtime.prepare_resources());
}

// ============================================================================
// Section 32: merge_parallel_results additional edge cases
// ============================================================================

class ObDASFusionIterMergeEdgeCaseTest : public ::testing::Test
{
protected:
  ObDASFusionIterMergeEdgeCaseTest()
      : alloc_("TestMergeEdge"),
        exec_ctx_(alloc_),
        eval_ctx_(exec_ctx_),
        ctdef_(alloc_)
  {
    eval_ctx_.max_batch_size_ = 256;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;

    const int64_t frame_size = 1024 * 1024;
    exec_ctx_.frames_ = (char **)alloc_.alloc(2 * sizeof(char*));
    if (OB_NOT_NULL(exec_ctx_.frames_)) {
      exec_ctx_.frames_[0] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[0])) {
        memset(exec_ctx_.frames_[0], 0, frame_size);
      }
      exec_ctx_.frames_[1] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[1])) {
        memset(exec_ctx_.frames_[1], 0, frame_size);
      }
      exec_ctx_.frame_cnt_ = 2;
    }
    eval_ctx_.frames_ = exec_ctx_.frames_;

    rowid_expr_.obj_meta_.set_type(ObUInt64Type);
    rowid_expr_.datum_meta_.type_ = ObUInt64Type;
    rowid_expr_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    rowid_expr_.frame_idx_ = 0;
    int64_t pos = 0;
    rowid_expr_.datum_off_ = pos; pos += sizeof(ObDatum);
    rowid_expr_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    rowid_expr_.res_buf_off_ = pos; rowid_expr_.res_buf_len_ = 8; pos += 8;
    rowid_expr_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    score0_.obj_meta_.set_type(ObDoubleType);
    score0_.datum_meta_.type_ = ObDoubleType;
    score0_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    score0_.frame_idx_ = 0;
    score0_.datum_off_ = pos; pos += sizeof(ObDatum);
    score0_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    score0_.res_buf_off_ = pos; score0_.res_buf_len_ = 8; pos += 8;
    score0_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    score1_.obj_meta_.set_type(ObDoubleType);
    score1_.datum_meta_.type_ = ObDoubleType;
    score1_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    score1_.frame_idx_ = 0;
    score1_.datum_off_ = pos; pos += sizeof(ObDatum);
    score1_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    score1_.res_buf_off_ = pos; score1_.res_buf_len_ = 8; pos += 8;
    score1_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    fusion_score_.obj_meta_.set_type(ObDoubleType);
    fusion_score_.datum_meta_.type_ = ObDoubleType;
    fusion_score_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    fusion_score_.frame_idx_ = 0;
    fusion_score_.datum_off_ = pos; pos += sizeof(ObDatum);
    fusion_score_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    fusion_score_.res_buf_off_ = pos; fusion_score_.res_buf_len_ = 8; pos += 8;
    fusion_score_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
    ctdef_.children_cnt_ = 2;
    ctdef_.has_search_subquery_ = true;
    ctdef_.has_vector_subquery_ = true;
    ctdef_.set_search_index(0);
    ctdef_.rowid_exprs_.init(1);
    ctdef_.rowid_exprs_.push_back(&rowid_expr_);
    ctdef_.score_exprs_.init(3);
    ctdef_.score_exprs_.push_back(&score0_);
    ctdef_.score_exprs_.push_back(&score1_);
    ctdef_.score_exprs_.push_back(&fusion_score_);
    ctdef_.result_output_.init(4);
    ctdef_.result_output_.push_back(&rowid_expr_);
    ctdef_.result_output_.push_back(&score0_);
    ctdef_.result_output_.push_back(&score1_);
    ctdef_.result_output_.push_back(&fusion_score_);
  }

  ObArenaAllocator alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASFusionCtDef ctdef_;
  ObExpr rowid_expr_;
  ObExpr score0_;
  ObExpr score1_;
  ObExpr fusion_score_;
};

TEST_F(ObDASFusionIterMergeEdgeCaseTest,
       merge_with_inconsistent_no_limit_and_top_k)
{
  // 同一逻辑路径的物理 Slice 必须使用统一的路径级 TopK。
  ObDASFusionIter iter;

  ObDASFusionPathSliceWork rt0, rt1;
  rt0.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt0, 0, 0, -1);
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 0.9));

  rt1.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt1, 0, 1, 5);
  rt1.rows_.push_back(ObDASFusionMaterializedRow(2, 0.8));

  iter.parallel_ctx_.path_slice_works_.push_back(&rt0);
  iter.parallel_ctx_.path_slice_works_.push_back(&rt1);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.merge_parallel_results());

  iter.parallel_ctx_.path_slice_works_.reset();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ObDASFusionIterMergeEdgeCaseTest, merge_with_invalid_path_idx)
{
  // path_idx_ 越界的 Work 应当失败。
  ObDASFusionIter iter;

  ObDASFusionPathSliceWork rt;
  rt.exec_env_.path_idx_ = 99;
  rt.rows_.push_back(ObDASFusionMaterializedRow(1, 0.9));

  iter.parallel_ctx_.path_slice_works_.push_back(&rt);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.merge_parallel_results());

  iter.parallel_ctx_.path_slice_works_.reset();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ObDASFusionIterMergeEdgeCaseTest, merge_single_range_parallel_work)
{
  // 单个物理 Slice 不需要执行路径级 TopK 合并。
  ObDASFusionIter iter;

  ObDASFusionPathSliceWork rt;
  rt.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt, 0, 0, 3);
  rt.rows_.push_back(ObDASFusionMaterializedRow(10, 0.9));
  rt.rows_.push_back(ObDASFusionMaterializedRow(20, 0.8));

  iter.parallel_ctx_.path_slice_works_.push_back(&rt);

  ObDASFusionCtDef single_ctdef(alloc_);
  single_ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  single_ctdef.children_cnt_ = 1;
  single_ctdef.has_search_subquery_ = true;
  single_ctdef.set_search_index(0);
  single_ctdef.rowid_exprs_.init(1);
  single_ctdef.rowid_exprs_.push_back(&rowid_expr_);
  single_ctdef.score_exprs_.init(2);
  single_ctdef.score_exprs_.push_back(&score0_);
  single_ctdef.score_exprs_.push_back(&fusion_score_);

  iter.fusion_ctdef_ = &single_ctdef;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 1;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  // 只有一个 Work 时按未切分逻辑路径处理。
  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());
  EXPECT_EQ(2, iter.fusion_docs_.count());

  iter.parallel_ctx_.path_slice_works_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ObDASFusionIterMergeEdgeCaseTest,
       merge_partitioned_path_without_top_k_emits_all_rows)
{
  // A path without TopK preserves all Slice rows and their original order.
  ObDASFusionIter iter;

  ObDASFusionPathSliceWork rt0, rt1;
  rt0.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt0, 0, 0, -1);
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 0.1));

  rt1.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt1, 0, 1, -1);
  rt1.rows_.push_back(ObDASFusionMaterializedRow(2, 0.9));

  iter.parallel_ctx_.path_slice_works_.push_back(&rt0);
  iter.parallel_ctx_.path_slice_works_.push_back(&rt1);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());
  ASSERT_EQ(2, iter.fusion_docs_.count());
  EXPECT_EQ(1UL, iter.fusion_docs_.at(0).get_uint64_rowkey());
  EXPECT_EQ(2UL, iter.fusion_docs_.at(1).get_uint64_rowkey());

  iter.parallel_ctx_.path_slice_works_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ObDASFusionIterMergeEdgeCaseTest, merge_mixed_range_parallel_and_normal)
{
  // 混合验证同一路径的 Range 并行 Work 和另一条路径的普通 Work。
  ObDASFusionIter iter;

  // 路径 0 包含两个 Range 并行 Work。
  ObDASFusionPathSliceWork rt0, rt1, rt2;
  rt0.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt0, 0, 0, 2);
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 100.0));
  rt0.rows_.push_back(ObDASFusionMaterializedRow(3, 97.0));

  rt1.exec_env_.path_idx_ = 0;
  init_docid_range_slice(rt1, 0, 1, 2);
  rt1.rows_.push_back(ObDASFusionMaterializedRow(2, 99.0));
  rt1.rows_.push_back(ObDASFusionMaterializedRow(4, 96.0));

  // 路径 1 包含一个普通的非 Range 并行 Work。
  rt2.exec_env_.path_idx_ = 1;
  ASSERT_EQ(OB_SUCCESS, rt2.work_slice_.init_whole_path(1));
  rt2.rows_.push_back(ObDASFusionMaterializedRow(1, 0.8));
  rt2.rows_.push_back(ObDASFusionMaterializedRow(5, 0.7));

  iter.parallel_ctx_.path_slice_works_.push_back(&rt0);
  iter.parallel_ctx_.path_slice_works_.push_back(&rt1);
  iter.parallel_ctx_.path_slice_works_.push_back(&rt2);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 5;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());

  // Path 0 has top-2 = {1, 2} after merge, plus path 1 has {1, 5}
  // So docs should be {1 (both paths), 2 (path 0 only), 5 (path 1 only)}
  // Note: 3 and 4 are cut by top-2 limit on path 0
  EXPECT_GE(iter.fusion_docs_.count(), 2);

  bool found_doc1 = false;
  bool found_doc2 = false;
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    const ObDASFusionDocInfo &doc = iter.fusion_docs_.at(i);
    uint64_t rk = doc.get_uint64_rowkey();
    if (rk == 1) {
      found_doc1 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
    } else if (rk == 2) {
      found_doc2 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_FALSE(doc.has_path(1));
    }
    // doc 3 and 4 should be filtered out by top-k limit
    EXPECT_NE(3ULL, rk);
    EXPECT_NE(4ULL, rk);
  }
  EXPECT_TRUE(found_doc1);
  EXPECT_TRUE(found_doc2);

  iter.parallel_ctx_.path_slice_works_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

// ============================================================================
// Section 33: ObDASFusionParallelCtx additional tests
// ============================================================================

TEST_F(ObDASFusionParallelCtxTest, discover_shared_bitmaps_dop_less_than_1)
{
  ObDASFusionParallelCtx ctx;
  // dop <= 1 should skip discovery
  int dummy = 0;
  ObDASIter *fake_iter = reinterpret_cast<ObDASIter *>(&dummy);
  EXPECT_EQ(OB_SUCCESS, ctx.discover_shared_bitmaps(alloc_, fake_iter, 0));
  EXPECT_FALSE(ctx.has_shared_bitmaps());

  EXPECT_EQ(OB_SUCCESS, ctx.discover_shared_bitmaps(alloc_, fake_iter, -1));
  EXPECT_FALSE(ctx.has_shared_bitmaps());
}

TEST_F(ObDASFusionParallelCtxTest, get_path_slice_work_out_of_bounds)
{
  ObDASFusionParallelCtx ctx;
  EXPECT_EQ(nullptr, ctx.get_path_slice_work(-1));
  EXPECT_EQ(nullptr, ctx.get_path_slice_work(0));
  EXPECT_EQ(nullptr, ctx.get_path_slice_work(999));
}

// ============================================================================
// 第 34 节：ObDASFusionWorkUnitTask 补充初始化校验
// ============================================================================

TEST_F(ObDASFusionWorkUnitTaskTest, init_with_zero_timeout)
{
  ObDASFusionWorkUnitTask task;
  ObDASFusionWorkUnit work_unit;
  TestFusionWork work(
      0, ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE);
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));
  ASSERT_EQ(OB_SUCCESS,
            work_unit.init(0, 0, work));

  // Zero timeout is technically valid (means already expired)
  ASSERT_EQ(OB_SUCCESS, task.init(&work_unit, &coord, 0, 0));
  EXPECT_EQ(0, task.get_timeout_ts());
}

TEST_F(ObDASFusionWorkUnitTaskTest, init_with_negative_timeout)
{
  ObDASFusionWorkUnitTask task;
  ObDASFusionWorkUnit work_unit;
  TestFusionWork work(
      0, ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE);
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));
  ASSERT_EQ(OB_SUCCESS,
            work_unit.init(0, 0, work));

  // Negative timeout (already expired)
  ASSERT_EQ(OB_SUCCESS, task.init(&work_unit, &coord, -1, 0));
  EXPECT_EQ(-1, task.get_timeout_ts());
}

// ============================================================================
// Section 35: ObDASFusionMaterializedRow additional edge cases
// ============================================================================

TEST(ObDASFusionMaterializedRowAdditionalTest, copy_and_assign)
{
  ObDASFusionMaterializedRow row1(42, 3.14);
  ObDASFusionMaterializedRow row2 = row1;
  EXPECT_EQ(42UL, row2.rowkey_);
  EXPECT_DOUBLE_EQ(3.14, row2.score_);

  ObDASFusionMaterializedRow row3;
  row3 = row1;
  EXPECT_EQ(42UL, row3.rowkey_);
  EXPECT_DOUBLE_EQ(3.14, row3.score_);
}

TEST(ObDASFusionMaterializedRowAdditionalTest, negative_score)
{
  ObDASFusionMaterializedRow row(100, -5.5);
  EXPECT_EQ(100UL, row.rowkey_);
  EXPECT_DOUBLE_EQ(-5.5, row.score_);
}

TEST(ObDASFusionMaterializedRowAdditionalTest, very_large_rowkey)
{
  const uint64_t max_key = UINT64_MAX - 1;
  ObDASFusionMaterializedRow row(max_key, 1.0);
  EXPECT_EQ(max_key, row.rowkey_);
}

// ============================================================================
// Section 36: ObDASBitmapOp external bitmap and drain_until tests
// ============================================================================

class EmptyBatchRowIterator : public ObNewRowIterator
{
public:
  EmptyBatchRowIterator() : batch_calls_(0) {}

  int get_next_row(ObNewRow *&row) override
  {
    row = nullptr;
    return OB_ITER_END;
  }

  int get_next_rows(int64_t &count, int64_t capacity) override
  {
    UNUSED(capacity);
    ++batch_calls_;
    count = 0;
    return OB_ITER_END;
  }

  void reset() override { batch_calls_ = 0; }

  int64_t batch_calls_;
};

class FixedBatchRowIterator : public ObNewRowIterator
{
public:
  FixedBatchRowIterator(uint64_t *batch_data,
                        const uint64_t *source_data,
                        int64_t source_count)
    : batch_data_(batch_data),
      source_data_(source_data),
      source_count_(source_count),
      source_offset_(0),
      batch_calls_(0)
  {}

  int get_next_row(ObNewRow *&row) override
  {
    row = nullptr;
    return OB_NOT_SUPPORTED;
  }

  int get_next_rows(int64_t &count, int64_t capacity) override
  {
    ++batch_calls_;
    count = 0;
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(capacity <= 0 || OB_ISNULL(batch_data_))) {
      ret = OB_INVALID_ARGUMENT;
    } else if (source_offset_ >= source_count_) {
      ret = OB_ITER_END;
    } else {
      count = std::min(capacity, source_count_ - source_offset_);
      MEMCPY(batch_data_, source_data_ + source_offset_, count * sizeof(uint64_t));
      source_offset_ += count;
      if (source_offset_ == source_count_) {
        // Exercise the storage convention of returning the terminal partial
        // batch together with OB_ITER_END.
        ret = OB_ITER_END;
      }
    }
    return ret;
  }

  void reset() override
  {
    source_offset_ = 0;
    batch_calls_ = 0;
  }

  uint64_t *batch_data_;
  const uint64_t *source_data_;
  int64_t source_count_;
  int64_t source_offset_;
  int64_t batch_calls_;
};

TEST(ObDASBitmapOpBuildTest, fixed_vector_batches_use_bulk_bitmap_path)
{
  static const int64_t BATCH_SIZE = 3;
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapBuild");
  ObDASScanOp mock_scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  eval_ctx.max_batch_size_ = BATCH_SIZE;
  search_ctx.eval_ctx_ = &eval_ctx;

  uint64_t batch_data[BATCH_SIZE] = {};
  const uint64_t source_data[] = {
      ObFastBitmap::CHUNK_SIZE + 9, 65, 0, 65,
      ObFastBitmap::CHUNK_SIZE + 3, 64, 2};
  FixedBatchRowIterator fixed_iter(
      batch_data, source_data, ARRAYSIZEOF(source_data));

  char **frames = static_cast<char **>(alloc.alloc(sizeof(char *)));
  ASSERT_NE(nullptr, frames);
  frames[0] = static_cast<char *>(alloc.alloc(sizeof(VectorHeader)));
  ASSERT_NE(nullptr, frames[0]);
  exec_ctx.frames_ = frames;
  eval_ctx.frames_ = frames;

  void *null_buf = alloc.alloc(ObBitVector::memory_size(BATCH_SIZE));
  ASSERT_NE(nullptr, null_buf);
  ObBitVector *nulls = reinterpret_cast<ObBitVector *>(null_buf);
  nulls->init(BATCH_SIZE);

  VectorHeader *vector_header = new (frames[0]) VectorHeader();
  vector_header->set_format(VEC_FIXED);
  new (vector_header->vector_buf_)
      ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_UINTEGER>>(
          reinterpret_cast<char *>(batch_data), nulls);

  ObExpr rowid_expr;
  rowid_expr.frame_idx_ = 0;
  rowid_expr.vector_header_off_ = 0;

  ObDASScalarScanOp scalar_scan_op(search_ctx);
  scalar_scan_op.result_ = &fixed_iter;
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(0));
  ObDASBitmapOp bitmap_op(search_ctx);
  bitmap_op.input_op_ = &scalar_scan_op;
  bitmap_op.rowid_expr_ = &rowid_expr;

  ASSERT_EQ(OB_SUCCESS, bitmap_op.fill_bitmap_from_scan(bitmap));
  EXPECT_EQ(4, fixed_iter.batch_calls_);
  EXPECT_FALSE(bitmap.stats_dirty_);
  EXPECT_EQ(6, bitmap.cardinality());

  const uint64_t expected[] = {
      0, 2, 64, 65,
      ObFastBitmap::CHUNK_SIZE + 3, ObFastBitmap::CHUNK_SIZE + 9};
  ObFastBitmap::Iterator bitmap_iter;
  ASSERT_EQ(OB_SUCCESS, bitmap_iter.init(&bitmap));
  for (uint64_t value : expected) {
    uint64_t actual = UINT64_MAX;
    ASSERT_EQ(OB_SUCCESS, bitmap_iter.next_id(actual));
    EXPECT_EQ(value, actual);
  }
  uint64_t actual = UINT64_MAX;
  EXPECT_EQ(OB_ITER_END, bitmap_iter.next_id(actual));
}

TEST(ObDASBitmapOpBuildTest, empty_scan_is_built_once)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapBuild");
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  eval_ctx.max_batch_size_ = 16;
  search_ctx.eval_ctx_ = &eval_ctx;

  EmptyBatchRowIterator empty_iter;
  ObDASScalarScanOp scalar_scan_op(search_ctx);
  scalar_scan_op.result_ = &empty_iter;
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(16));

  ObDASBitmapOp bitmap_op(search_ctx);
  bitmap_op.input_op_ = &scalar_scan_op;
  bitmap_op.bitmap_ = &bitmap;
  ASSERT_FALSE(bitmap_op.bitmap_built_);
  ASSERT_EQ(OB_SUCCESS, bitmap_op.build_bitmap());
  EXPECT_TRUE(bitmap_op.bitmap_built_);
  EXPECT_TRUE(bitmap_op.exhausted_);
  EXPECT_EQ(1, empty_iter.batch_calls_);

  ObDASRowID boundary;
  boundary.set_max();
  uint64_t ids[2] = {};
  int64_t count = 1;
  ObDASRowID next_id;
  double next_score = 1.0;
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, nullptr,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);
  EXPECT_TRUE(next_id.is_max());
  EXPECT_DOUBLE_EQ(0.0, next_score);
  EXPECT_EQ(1, empty_iter.batch_calls_);
}

TEST(ObDASBitmapOpSetExternalTest, set_null_bitmap_fails)
{
  // set_external_bitmap should fail with null bitmap
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(scan_alloc, mock_scan_op);
  ObDASBitmapOp bitmap_op(search_ctx);

  EXPECT_EQ(OB_INVALID_ARGUMENT, bitmap_op.set_external_bitmap(nullptr));
}

TEST(ObDASBitmapOpSetExternalTest, open_rejects_unfinalized_bitmap)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapOpen");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(0));
  const uint64_t values[] = {1, 2};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values, ARRAYSIZEOF(values)));

  ObDASScanOp mock_scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  ObDASBitmapOp bitmap_op(search_ctx);
  ASSERT_EQ(OB_SUCCESS, bitmap_op.set_external_bitmap(&bitmap));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bitmap_op.do_open());
  EXPECT_FALSE(bitmap_op.bitmap_built_);

  bitmap.finalize();
  ASSERT_EQ(OB_SUCCESS, bitmap_op.do_open());
  EXPECT_TRUE(bitmap_op.bitmap_built_);
  EXPECT_FALSE(bitmap_op.exhausted_);
  EXPECT_EQ(OB_SUCCESS, bitmap_op.do_close());
}

TEST(ObDASBitmapOpDrainUntilTest, non_uint64_operator_scope_is_rejected)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapDrain");
  ObDASScanOp mock_scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  ASSERT_TRUE(search_ctx.is_uint64_rowid());
  search_ctx.scopes_[DAS_SCOPE_NESTED_SYNC].rowid_type_ =
      DAS_ROWID_TYPE_COMPACT;

  ObDASBitmapOp bitmap_op(search_ctx);
  bitmap_op.scope_ = DAS_SCOPE_NESTED_SYNC;
  ObDASRowID boundary;
  boundary.set_max();
  uint64_t ids[2] = {};
  int64_t count = 1;
  ObDASRowID next_id;
  double next_score = 1.0;
  EXPECT_EQ(OB_NOT_SUPPORTED,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, nullptr,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);
  EXPECT_DOUBLE_EQ(0.0, next_score);
}

TEST(ObDASBitmapOpDrainUntilTest, boundary_prefetch_and_score)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapDrain");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  const uint64_t values[] = {1, 3, 5, 7};
  for (uint64_t value : values) {
    ASSERT_EQ(OB_SUCCESS, bitmap.add(value));
  }

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  ObDASBitmapOp bitmap_op(search_ctx);
  bitmap_op.is_scoring_ = true;
  bitmap_op.boost_weight_ = 2.5;
  ASSERT_EQ(OB_SUCCESS, bitmap_op.set_external_bitmap(&bitmap));
  ASSERT_EQ(OB_SUCCESS, bitmap_op.do_open());

  ObDASRowID curr_id;
  double curr_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bitmap_op.next_rowid(curr_id, curr_score));
  EXPECT_EQ(1ULL, curr_id.get_uint64());
  EXPECT_DOUBLE_EQ(2.5, curr_score);

  ObDASRowID boundary;
  boundary.set_uint64(5);
  uint64_t ids[8] = {};
  double scores[8] = {};
  int64_t count = 0;
  ObDASRowID next_id;
  double next_score = 0.0;
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  ASSERT_EQ(2, count);
  EXPECT_EQ(3ULL, ids[0]);
  EXPECT_EQ(5ULL, ids[1]);
  EXPECT_DOUBLE_EQ(2.5, scores[0]);
  EXPECT_DOUBLE_EQ(2.5, scores[1]);
  ASSERT_TRUE(next_id.is_normal());
  EXPECT_EQ(7ULL, next_id.get_uint64());
  EXPECT_DOUBLE_EQ(2.5, next_score);

  // The raw operator cursor is on the prefetched row. Its owner must cache
  // next_id; calling next_rowid() directly advances past it.
  EXPECT_EQ(OB_ITER_END, bitmap_op.next_rowid(curr_id, curr_score));
  EXPECT_EQ(OB_SUCCESS, bitmap_op.do_close());
}

TEST(ObDASBitmapOpDrainUntilTest, full_batch_can_continue_with_same_boundary)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapDrain");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  const uint64_t values[] = {1, 3, 5, 7};
  for (uint64_t value : values) {
    ASSERT_EQ(OB_SUCCESS, bitmap.add(value));
  }

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  common::ObOpProfile<common::ObMetric> profile(
      common::ObProfileId::HYBRID_SEARCH_BITMAP, &alloc);
  ObDASBitmapOp bitmap_op(search_ctx);
  bitmap_op.is_scoring_ = false;
  bitmap_op.profile_ = &profile;
  ASSERT_EQ(OB_SUCCESS, bitmap_op.set_external_bitmap(&bitmap));
  ASSERT_EQ(OB_SUCCESS, bitmap_op.do_open());

  ObDASRowID boundary;
  boundary.set_max();
  uint64_t ids[2] = {};
  double scores[2] = {1.0, 1.0};
  int64_t count = 0;
  ObDASRowID next_id;
  double next_score = 1.0;
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  ASSERT_EQ(2, count);
  EXPECT_EQ(1ULL, ids[0]);
  EXPECT_EQ(3ULL, ids[1]);
  EXPECT_DOUBLE_EQ(0.0, scores[0]);
  EXPECT_DOUBLE_EQ(0.0, scores[1]);
  EXPECT_DOUBLE_EQ(0.0, next_score);
  bool metric_exists = false;
  uint64_t output_count = 0;
  profile.get_metric_value(
      common::ObMetricId::HS_OUTPUT_ROW_COUNT, metric_exists, output_count);
  EXPECT_TRUE(metric_exists);
  EXPECT_EQ(2ULL, output_count);

  scores[0] = 1.0;
  scores[1] = 1.0;
  next_score = 1.0;
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  ASSERT_EQ(2, count);
  EXPECT_EQ(5ULL, ids[0]);
  EXPECT_EQ(7ULL, ids[1]);
  EXPECT_DOUBLE_EQ(0.0, scores[0]);
  EXPECT_DOUBLE_EQ(0.0, scores[1]);
  EXPECT_DOUBLE_EQ(0.0, next_score);
  profile.get_metric_value(
      common::ObMetricId::HS_OUTPUT_ROW_COUNT, metric_exists, output_count);
  EXPECT_TRUE(metric_exists);
  EXPECT_EQ(4ULL, output_count);

  next_score = 1.0;
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);
  EXPECT_TRUE(next_id.is_max());
  EXPECT_DOUBLE_EQ(0.0, next_score);
  profile.get_metric_value(
      common::ObMetricId::HS_OUTPUT_ROW_COUNT, metric_exists, output_count);
  EXPECT_TRUE(metric_exists);
  EXPECT_EQ(4ULL, output_count);
  EXPECT_EQ(OB_SUCCESS, bitmap_op.do_close());
}

TEST(ObDASBitmapOpDrainUntilTest, docid_range_includes_pending_first)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapDrain");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  const uint64_t values[] = {1, 3, 5, 7, 9};
  for (uint64_t value : values) {
    ASSERT_EQ(OB_SUCCESS, bitmap.add(value));
  }

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  ObObj range_lo;
  ObObj range_hi;
  range_lo.set_uint64(3);
  range_hi.set_uint64(7);
  search_ctx.set_docid_range(range_lo, range_hi);

  ObDASBitmapOp bitmap_op(search_ctx);
  bitmap_op.is_scoring_ = true;
  bitmap_op.boost_weight_ = 1.75;
  ASSERT_EQ(OB_SUCCESS, bitmap_op.set_external_bitmap(&bitmap));
  ASSERT_EQ(OB_SUCCESS, bitmap_op.do_open());

  ObDASRowID boundary;
  boundary.set_max();
  uint64_t ids[8] = {};
  double scores[8] = {};
  int64_t count = 0;
  ObDASRowID next_id;
  double next_score = 0.0;
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  ASSERT_EQ(3, count);
  EXPECT_EQ(3ULL, ids[0]);
  EXPECT_EQ(5ULL, ids[1]);
  EXPECT_EQ(7ULL, ids[2]);
  EXPECT_DOUBLE_EQ(1.75, scores[0]);
  EXPECT_DOUBLE_EQ(1.75, scores[1]);
  EXPECT_DOUBLE_EQ(1.75, scores[2]);
  EXPECT_TRUE(next_id.is_max());
  EXPECT_DOUBLE_EQ(0.0, next_score);

  ASSERT_EQ(OB_SUCCESS, bitmap_op.do_rescan());
  boundary.set_uint64(1);
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);
  ASSERT_TRUE(next_id.is_normal());
  EXPECT_EQ(3ULL, next_id.get_uint64());
  EXPECT_DOUBLE_EQ(1.75, next_score);

  ASSERT_EQ(OB_SUCCESS, bitmap_op.do_rescan());
  ObDASRowID first_id;
  double first_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bitmap_op.next_rowid(first_id, first_score));
  EXPECT_EQ(3ULL, first_id.get_uint64());
  EXPECT_DOUBLE_EQ(1.75, first_score);
  EXPECT_EQ(OB_SUCCESS, bitmap_op.do_close());
}

TEST(ObDASBitmapOpDrainUntilTest, invalid_arguments_do_not_advance)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapDrain");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(0));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(2));

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  ObDASBitmapOp bitmap_op(search_ctx);
  bitmap_op.is_scoring_ = true;
  bitmap_op.boost_weight_ = 4.0;
  ASSERT_EQ(OB_SUCCESS, bitmap_op.set_external_bitmap(&bitmap));
  ASSERT_EQ(OB_SUCCESS, bitmap_op.do_open());

  uint64_t ids[2] = {};
  double scores[2] = {};
  int64_t count = 1;
  ObDASRowID next_id;
  double next_score = 1.0;

  ObDASRowID min_boundary;
  min_boundary.set_min();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      bitmap_op.drain_until(min_boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);
  EXPECT_DOUBLE_EQ(0.0, next_score);

  ObDASRowID max_boundary;
  max_boundary.set_max();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      bitmap_op.drain_until(max_boundary, 0, ids, scores,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      bitmap_op.drain_until(max_boundary, ARRAYSIZEOF(ids), nullptr, scores,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);

  ObDASRowID first_id;
  double first_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, bitmap_op.next_rowid(first_id, first_score));
  EXPECT_EQ(0ULL, first_id.get_uint64());
  EXPECT_DOUBLE_EQ(4.0, first_score);
  EXPECT_EQ(OB_SUCCESS, bitmap_op.do_close());
}

TEST(ObDASBitmapOpDrainUntilTest, boundary_before_first_prefetches_first)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapDrain");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(2));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(4));

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  ObDASBitmapOp bitmap_op(search_ctx);
  bitmap_op.is_scoring_ = true;
  bitmap_op.boost_weight_ = 3.0;
  ASSERT_EQ(OB_SUCCESS, bitmap_op.set_external_bitmap(&bitmap));
  ASSERT_EQ(OB_SUCCESS, bitmap_op.do_open());

  ObDASRowID boundary;
  boundary.set_uint64(0);
  uint64_t ids[2] = {};
  double scores[2] = {};
  int64_t count = 0;
  ObDASRowID next_id;
  double next_score = 0.0;
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);
  ASSERT_TRUE(next_id.is_normal());
  EXPECT_EQ(2ULL, next_id.get_uint64());
  EXPECT_DOUBLE_EQ(3.0, next_score);
  EXPECT_EQ(OB_SUCCESS, bitmap_op.do_close());
}

TEST(ObDASBitmapOpDrainUntilTest, empty_external_bitmap_returns_eof)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator alloc("BitmapDrain");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);
  ObDASBitmapOp bitmap_op(search_ctx);
  ASSERT_EQ(OB_SUCCESS, bitmap_op.set_external_bitmap(&bitmap));
  ASSERT_EQ(OB_SUCCESS, bitmap_op.do_open());

  ObDASRowID boundary;
  boundary.set_max();
  uint64_t ids[2] = {};
  double scores[2] = {};
  int64_t count = 1;
  ObDASRowID next_id;
  double next_score = 1.0;
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);
  EXPECT_TRUE(next_id.is_max());
  EXPECT_DOUBLE_EQ(0.0, next_score);

  count = 1;
  next_score = 1.0;
  ASSERT_EQ(OB_SUCCESS,
      bitmap_op.drain_until(boundary, ARRAYSIZEOF(ids), ids, scores,
                            count, next_id, next_score));
  EXPECT_EQ(0, count);
  EXPECT_TRUE(next_id.is_max());
  EXPECT_DOUBLE_EQ(0.0, next_score);
  EXPECT_EQ(OB_SUCCESS, bitmap_op.do_close());
}

// ============================================================================
// Section 37: ObDASFusionParallelCoordinator reset semantics
// ============================================================================

TEST_F(ObDASFusionParallelCoordinatorTest, reset_then_reinit_with_different_count)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(5));

  coord.on_work_unit_finish(OB_SUCCESS);
  coord.on_work_unit_finish(OB_ERR_UNEXPECTED);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());

  coord.reset();
  // After reset, counts should be zeroed, but is_inited_ should remain true
  // per the implementation comment: "Intentionally keep is_inited_ = true"
  // so that the cond_ can be reused.
  EXPECT_EQ(0, coord.total_cnt_);
  EXPECT_EQ(0, coord.finished_cnt_);
  EXPECT_EQ(OB_SUCCESS, coord.first_err_code_);

  // Re-init with different count
  ASSERT_EQ(OB_SUCCESS, coord.init(3));
  EXPECT_EQ(3, coord.total_cnt_);
  EXPECT_EQ(0, coord.finished_cnt_);

  coord.on_work_unit_finish(OB_SUCCESS);
  coord.on_work_unit_finish(OB_SUCCESS);
  coord.on_work_unit_finish(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

// ============================================================================
// 第 38 节：具体 Work 类型的默认值
// ============================================================================

TEST_F(ObDASFusionTaskExecEnvTest, work_slice_default_invalid)
{
  ObDASFusionPathSliceWork work;
  EXPECT_FALSE(work.work_slice_.is_valid());
  EXPECT_FALSE(work.is_partitioned_path());
  EXPECT_EQ(nullptr, work.slice_provider_);
}

TEST_F(ObDASFusionTaskExecEnvTest, docid_range_defaults)
{
  ObDASFusionPathSliceWork work;
  EXPECT_TRUE(work.work_slice_.docid_range_.lo_.is_null());
  EXPECT_TRUE(work.work_slice_.docid_range_.hi_.is_null());
}

TEST_F(ObDASFusionTaskExecEnvTest, parallel_ctx_default_null)
{
  ObDASFusionBitmapPrepareWork prepare_work;
  ObDASFusionPathSliceWork path_work;
  EXPECT_EQ(nullptr, prepare_work.parallel_ctx_);
  EXPECT_EQ(nullptr, path_work.parallel_ctx_);
  EXPECT_EQ(0, prepare_work.child_bitmap_ops_.count());
  EXPECT_EQ(0, path_work.child_bitmap_ops_.count());
}

// ============================================================================
// Section 39: ObSharedBitmapSlot lifecycle
// ============================================================================

TEST(ObSharedBitmapSlotLifecycleTest, release_null_bitmap_is_safe)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  // bitmap_ is null by default; release should not crash
  slot.release();
  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

TEST(ObSharedBitmapSlotLifecycleTest, build_ret_and_is_built_states)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);

  // Simulate a build failure
  ATOMIC_STORE(&slot.is_built_, true);
  slot.build_ret_ = OB_ALLOCATE_MEMORY_FAILED;
  EXPECT_TRUE(slot.is_built_);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, slot.build_ret_);

  // Release should reset
  slot.release();
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

// ============================================================================
// 第 40 节：ObDASFusionPathSliceWork::reset_result 清理结果行
// ============================================================================

TEST_F(ObDASFusionTaskExecEnvTest, reset_result_preserves_non_result_state)
{
  ObDASFusionPathSliceWork work;
  work.exec_env_.path_idx_ = 3;
  init_docid_range_slice(work, 3, 1, 42);
  work.rows_.push_back(ObDASFusionMaterializedRow(1, 1.0));

  work.reset_result();

  // reset_result 只清理业务结果。
  EXPECT_EQ(0, work.rows_.count());

  // But should preserve non-result state
  EXPECT_EQ(3, work.get_path_id());
  EXPECT_TRUE(work.is_partitioned_path());
  EXPECT_EQ(1, work.work_slice_.slice_id_);
  EXPECT_EQ(42, work.work_slice_.path_top_k_limit_);
}

// ============================================================================
// Section 41: ObDASFusionParallelCtx release releases bitmap slots
// ============================================================================

TEST_F(ObDASFusionParallelCtxTest, release_with_bitmap_slots)
{
  // Verify that release properly cleans up shared bitmap slots
  ObDASFusionParallelCtx ctx;
  ObArenaAllocator alloc("TestBmpSlot");

  // Manually create a shared bitmap slot
  ObSharedBitmapSlot *slot = nullptr;
  void *slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot));
  ASSERT_NE(nullptr, slot_buf);
  slot = new (slot_buf) ObSharedBitmapSlot();
  slot->bitmap_occurrence_idx_ = 0;
  ATOMIC_STORE(&slot->is_built_, false);

  ctx.shared_bitmap_slots_.push_back(slot);
  EXPECT_EQ(1, ctx.get_shared_bitmap_count());
  EXPECT_TRUE(ctx.has_shared_bitmaps());

  // Release should clean up the slot
  ctx.release();
  EXPECT_EQ(0, ctx.get_shared_bitmap_count());
  EXPECT_FALSE(ctx.has_shared_bitmaps());
  EXPECT_EQ(0, ctx.get_path_slice_work_count());
}

// ============================================================================
// Section 42: ObOpProfile::adopt_child tests
// ============================================================================
// adopt_child links an externally-allocated child profile into this profile's
// child list. The child keeps its own allocator; only a ProfileWrap is allocated
// from the parent's alloc_. Used by ObDASFusionIter to attach per-task profiles
// (allocated on a thread-safe arena) under the fusion profile.

class ObOpProfileAdoptChildTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObOpProfileAdoptChildTest, adopt_null_child_fails)
{
  ObArenaAllocator alloc("TestAdopt");
  common::ObOpProfile<common::ObMetric> parent(
      common::ObProfileId::HYBRID_SEARCH_FUSION_ITER, &alloc);

  EXPECT_EQ(OB_INVALID_ARGUMENT, parent.adopt_child(nullptr));
  EXPECT_EQ(nullptr, parent.get_child_head());
  EXPECT_EQ(0, parent.child_array_.count());
}

TEST_F(ObOpProfileAdoptChildTest, adopt_single_child_success)
{
  ObArenaAllocator parent_alloc("TestParent");
  ObArenaAllocator child_alloc("TestChild");
  common::ObOpProfile<common::ObMetric> parent(
      common::ObProfileId::HYBRID_SEARCH_FUSION_ITER, &parent_alloc);
  common::ObOpProfile<common::ObMetric> child(
      common::ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, &child_alloc);

  EXPECT_EQ(nullptr, child.get_parent());
  EXPECT_EQ(nullptr, parent.get_child_head());

  ASSERT_EQ(OB_SUCCESS, parent.adopt_child(&child));

  // Parent pointer is set on the child
  EXPECT_EQ(&parent, child.get_parent());

  // child_head_ / child_tail_ / child_array_ all reference the child
  ASSERT_NE(nullptr, parent.get_child_head());
  EXPECT_EQ(&child, parent.get_child_head()->elem_);
  EXPECT_EQ(nullptr, parent.get_child_head()->next_);
  ASSERT_EQ(1, parent.child_array_.count());
  EXPECT_EQ(&child, parent.child_array_.at(0));

  // Child's allocator is preserved (parent does NOT own the child's storage)
  EXPECT_EQ(&child_alloc, child.alloc_);
}

TEST_F(ObOpProfileAdoptChildTest, adopt_multiple_children_chain_in_order)
{
  ObArenaAllocator parent_alloc("TestParent");
  ObArenaAllocator child_alloc("TestChild");
  common::ObOpProfile<common::ObMetric> parent(
      common::ObProfileId::HYBRID_SEARCH_FUSION_ITER, &parent_alloc);
  common::ObOpProfile<common::ObMetric> child0(
      common::ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, &child_alloc);
  common::ObOpProfile<common::ObMetric> child1(
      common::ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, &child_alloc);
  common::ObOpProfile<common::ObMetric> child2(
      common::ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, &child_alloc);

  ASSERT_EQ(OB_SUCCESS, parent.adopt_child(&child0));
  ASSERT_EQ(OB_SUCCESS, parent.adopt_child(&child1));
  ASSERT_EQ(OB_SUCCESS, parent.adopt_child(&child2));

  // child_array_ preserves insertion order
  ASSERT_EQ(3, parent.child_array_.count());
  EXPECT_EQ(&child0, parent.child_array_.at(0));
  EXPECT_EQ(&child1, parent.child_array_.at(1));
  EXPECT_EQ(&child2, parent.child_array_.at(2));

  // Linked list (head -> next -> next) follows insertion order
  auto *wrap0 = parent.get_child_head();
  ASSERT_NE(nullptr, wrap0);
  EXPECT_EQ(&child0, wrap0->elem_);
  auto *wrap1 = wrap0->next_;
  ASSERT_NE(nullptr, wrap1);
  EXPECT_EQ(&child1, wrap1->elem_);
  auto *wrap2 = wrap1->next_;
  ASSERT_NE(nullptr, wrap2);
  EXPECT_EQ(&child2, wrap2->elem_);
  EXPECT_EQ(nullptr, wrap2->next_);

  EXPECT_EQ(&parent, child0.get_parent());
  EXPECT_EQ(&parent, child1.get_parent());
  EXPECT_EQ(&parent, child2.get_parent());

  // Counts traversal walks the adopted subtree
  int64_t metric_count = 0;
  int64_t profile_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, parent.get_all_count(metric_count, profile_cnt));
  EXPECT_EQ(3, profile_cnt);
}

// ============================================================================
// Section 43: TODO tests for functions requiring complex infrastructure
// ============================================================================

// compute_split_points() obtains ranges through MTL(ObAccessService*).
// Its pure conversion is covered by the SliceProvider tests above; storage
// acquisition requires an integration environment.

TEST(FusionIVFTabletTest, worker_copy_preserves_current_partition_tablets)
{
  ObArenaAllocator allocator;
  ObDASVecIndexIVFScanIter source_scan;
  ObDASVecIndexDriverIter source_driver;
  source_driver.vec_index_scan_iter_ = &source_scan;
  for (uint64_t partition = 1; partition <= 2; ++partition) {
    ObDASRelatedTabletID current(allocator);
    current.centroid_tablet_id_ = ObTabletID(1000 * partition + 1);
    current.cid_vec_tablet_id_ = ObTabletID(1000 * partition + 2);
    current.rowkey_cid_tablet_id_ = ObTabletID(1000 * partition + 3);
    current.lookup_tablet_id_ = ObTabletID(1000 * partition + 4);
    current.special_aux_tablet_id_ = ObTabletID(1000 * partition + 5);
    source_driver.set_related_tablet_ids(current);
    source_scan.set_related_tablet_ids(current);

    ObDASRelatedTabletID copied(allocator);
    source_driver.fill_related_tablet_ids(copied);
    ObDASVecIndexIVFScanIter worker_scan;
    worker_scan.set_related_tablet_ids(copied);
    EXPECT_EQ(current.centroid_tablet_id_, worker_scan.centroid_tablet_id_);
    EXPECT_EQ(current.cid_vec_tablet_id_, worker_scan.cid_vec_tablet_id_);
    EXPECT_EQ(current.rowkey_cid_tablet_id_, worker_scan.rowkey_cid_tablet_id_);
    EXPECT_EQ(current.lookup_tablet_id_, worker_scan.com_aux_tablet_id_);
    EXPECT_EQ(current.special_aux_tablet_id_, worker_scan.special_aux_tablet_id_);
  }
}

// TODO: create_parallel_iter() vector index path - requires ObDASVecIndexDriverIter
// mock infrastructure. The path involves:
//   1. fill_related_tablet_ids / set_related_tablet_ids propagation
//   2. deep_copy_rtdef_tree with vec ctdef/rtdef
//   3. ObDASIterUtils::create_vec_search_iter
//
// TEST(CreateParallelIterVecTest, tablet_id_propagation) { ... }

// TODO: swizzle_child_rtdef() with different op types - requires constructing
// full rtdef trees for DAS_OP_TABLE_SCAN, DAS_OP_TABLE_BATCH_SCAN,
// DAS_OP_SCALAR_SCAN_QUERY which need ObDASTaskFactory, ObPhysicalPlan,
// and many other dependent objects.
//
// TEST(SwizzleChildRtdefTest, table_scan_type) { ... }
// TEST(SwizzleChildRtdefTest, table_batch_scan_type) { ... }
// TEST(SwizzleChildRtdefTest, scalar_scan_query_type) { ... }

// TODO：alloc_frames_safe() 是文件内静态函数，单测无法直接调用。
// 如需验证，需要增加封装接口，或者通过 ObDASFusionTaskExecEnv::
// create_fusion_child_eval_ctx 验证不同的 frame 配置：
//   1. 参数 frame 缺失，即 param_frame_.count() > param_frame_ptrs.count()
//   2. frame 数量为 0
//
// TEST(AllocFramesSafeTest, missing_param_frames) { ... }
// TEST(AllocFramesSafeTest, zero_frame_count) { ... }

// TODO: drain_child_iter() cancellation and timeout - requires a mock ObDASIter
// that responds to get_next_rows. The function checks:
//   1. coordinator.get_first_error() for cancellation
//   2. THIS_WORKER.is_timeout() for timeout
//   3. OB_ITER_END with partial batch handling
//
// TEST(DrainChildIterTest, cancellation_via_first_error) { ... }
// TEST(DrainChildIterTest, timeout_detection) { ... }
// TEST(DrainChildIterTest, iter_end_with_partial_batch) { ... }

// TODO: do_parallel_table_scan() partial task submission - requires full
// ObDASFusionIter setup with mock task scheduler. Some tasks submitted,
// some not.
//
// TEST(DoParallelTableScanTest, partial_submission) { ... }

// TEST(ObDASBitmapOpDoOpenTest, self_build_path) { ... }

// TODO: materialize_batch_result() rich-format (vectorized) and
// non-rich-format (datum) paths with actual expression evaluation.
// The null-score-datum path and batch_size edge cases are tested above
// via guards, but full expression evaluation testing requires:
//   1. Properly initialized ObExpr with vector format
//   2. ObEvalCtx with batch info
//   3. Actual datum values written to expression frames
//
// TEST(MaterializeBatchResultTest, rich_format_path) { ... }
// TEST(MaterializeBatchResultTest, datum_path_with_null_score) { ... }
// TEST(MaterializeBatchResultTest, batch_size_one) { ... }

TEST(ObFastBitmapAdaptiveTest, mixed_batch_sizes_and_locality)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  ObArenaAllocator alloc("AdaptiveBitmap");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(0));
  std::vector<uint64_t> expected;
  for (int64_t count : {1, 2, 7, 16, 31, 32, 63, 255, 256, 257, 513}) {
    for (int64_t run_length : {1, 2, 3, 4, 8, 16}) {
      std::vector<uint64_t> values(count);
      for (int64_t i = 0; i < count; ++i) {
        // Reverse chunk order, repeated words, and both sides of the decision threshold.
        const uint64_t word = (8191 - (i / run_length) * 17) & 8191;
        values[i] = (word << 6) + (i % run_length);
      }
      ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values.data(), count));
      expected.insert(expected.end(), values.begin(), values.end());
      // Check each batch before reuse so later batches cannot mask missing bits.
      bitmap.finalize();
      std::sort(expected.begin(), expected.end());
      expected.erase(std::unique(expected.begin(), expected.end()), expected.end());
      ASSERT_EQ(expected.size(), bitmap.cardinality());
      ObFastBitmap::Iterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
      uint64_t actual = 0;
      for (uint64_t value : expected) {
        ASSERT_EQ(OB_SUCCESS, iter.next_id(actual));
        ASSERT_EQ(value, actual);
      }
      ASSERT_EQ(OB_ITER_END, iter.next_id(actual));
      bitmap.reuse();
      expected.clear();
    }
  }
  const uint64_t boundaries[] = {0, 63, 64, 65535, 65536, 0, 64};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(boundaries, ARRAYSIZEOF(boundaries)));
  expected.insert(expected.end(), boundaries, boundaries + ARRAYSIZEOF(boundaries));
  bitmap.finalize();
  std::sort(expected.begin(), expected.end());
  expected.erase(std::unique(expected.begin(), expected.end()), expected.end());
  ASSERT_EQ(expected.size(), bitmap.cardinality());
  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  uint64_t actual = 0;
  for (uint64_t value : expected) {
    ASSERT_EQ(OB_SUCCESS, iter.next_id(actual));
    ASSERT_EQ(value, actual);
  }
  ASSERT_EQ(OB_ITER_END, iter.next_id(actual));
}

TEST(ObFastBitmapAdaptiveTest, changing_locality_and_reuse)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  ObArenaAllocator alloc("AdaptiveReuse");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(256));
  for (int64_t round = 0; round < 6; ++round) {
    std::vector<uint64_t> values(256);
    const int64_t dense_count = round * 48;
    for (int64_t i = 0; i < 256; ++i) {
      values[i] = i < dense_count ? static_cast<uint64_t>(i)
                                  : static_cast<uint64_t>((256 - i) * 65536 + i);
    }
    ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values.data(), 256));
    ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values.data(), 256));
    bitmap.finalize();
    std::sort(values.begin(), values.end());
    values.erase(std::unique(values.begin(), values.end()), values.end());
    ASSERT_EQ(values.size(), bitmap.cardinality());
    ObFastBitmap::Iterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
    uint64_t actual = 0;
    for (uint64_t value : values) {
      ASSERT_EQ(OB_SUCCESS, iter.next_id(actual));
      ASSERT_EQ(value, actual);
    }
    ASSERT_EQ(OB_ITER_END, iter.next_id(actual));
    bitmap.reuse();
  }
}

TEST(ObFastBitmapAdaptiveTest, nonuniform_word_locality)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  for (const bool dense_samples : {false, true}) {
    ObArenaAllocator alloc("BitmapNonUnif");
    ObFastBitmap bitmap(alloc);
    ASSERT_EQ(OB_SUCCESS, bitmap.init(0));
    std::vector<uint64_t> values;
    values.reserve(256);
    for (int64_t i = 0; i < 256; ++i) {
      uint64_t word = 0;
      uint64_t bit = 0;
      if (dense_samples) {
        // Only the sampled windows share words; most adjacent IDs do not.
        const int64_t run_start = i < 3 ? 0 : (i >= 127 && i < 130 ? 127 : (i >= 253 ? 253 : -1));
        word = run_start < 0 ? i : run_start;
        bit = run_start < 0 ? 0 : i - run_start;
      } else {
        // The sampled windows are sparse; most other IDs share words.
        word = i < 3 ? i : i < 65 ? 3 : i < 127 ? 4
            : i < 130 ? 5 + i - 127 : i < 192 ? 8 : i < 253 ? 9 : 10 + i - 253;
        bit = i < 3 || (i >= 127 && i < 130) || i >= 253 ? 0
            : i < 65 ? i - 3 : i < 127 ? i - 65 : i < 192 ? i - 130 : i - 192;
      }
      values.push_back((word << 6) + bit);
    }
    ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values.data(), values.size()));
    bitmap.finalize();
    ASSERT_EQ(values.size(), bitmap.cardinality());
    ObFastBitmap::Iterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
    for (const uint64_t value : values) {
      uint64_t actual = 0;
      ASSERT_EQ(OB_SUCCESS, iter.next_id(actual));
      ASSERT_EQ(value, actual);
    }
    uint64_t actual = 0;
    ASSERT_EQ(OB_ITER_END, iter.next_id(actual));
  }
}

TEST(ObFastBitmapFlatTest, source_fallback_preserves_chunk_reservation)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  ObArenaAllocator alloc("FlatReserveTest");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  class InputRangeOp : public TestEstimatedWorkSearchOp
  {
  public:
    explicit InputRangeOp(ObDASSearchCtx &ctx)
      : TestEstimatedWorkSearchOp(ctx, 0), count_(8843), hi_(36113228), available_(true) {}
    int get_bitmap_input_range(bool &available, uint64_t &lo,
                               uint64_t &hi, int64_t &estimated_count) override
    {
      available = available_;
      lo = 1;
      hi = hi_;
      estimated_count = count_;
      return OB_SUCCESS;
    }
    int64_t count_;
    uint64_t hi_;
    bool available_;
  } source(search_ctx);
  const int64_t fallback_estimate = 8841823;
  for (int64_t scenario = 0; scenario < 3; ++scenario) {
    // Sparse input, unknown domain, and an oversized flat array all use chunks.
    source.available_ = scenario != 1;
    source.hi_ = scenario == 2 ? ObFastBitmap::MAX_FLAT_BYTES * 8 + 65536 : 36113228;
    source.count_ = scenario == 2 ? source.hi_ : 8843;
    ObFastBitmap bitmap(alloc);
    ASSERT_EQ(OB_SUCCESS, bitmap.init_from_source(source, fallback_estimate));
    ASSERT_FALSE(bitmap.is_flat());
    ASSERT_EQ(135, bitmap.chunk_buf_count_);
    std::vector<uint64_t> values;
    for (uint64_t i = 0; i < 142; ++i) {
      values.push_back((i + 1) * ObFastBitmap::CHUNK_SIZE);
    }
    ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(values.data(), values.size()));
    bitmap.finalize();
    ASSERT_EQ(142, bitmap.cardinality());
    ASSERT_EQ(135, bitmap.chunk_buf_idx_);
  }
  // Preserving chunk reservation must not force sparse input into the flat layout
  // or prevent dense input from using it.
  source.available_ = true;
  source.hi_ = 36113228;
  source.count_ = fallback_estimate;
  ObFastBitmap dense_bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, dense_bitmap.init_from_source(source, fallback_estimate));
  ASSERT_TRUE(dense_bitmap.is_flat());
}

TEST(ObFastBitmapFlatTest, source_range_changes_preserve_results_and_reservation)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  ObArenaAllocator alloc("FlatRescanTest");
  ObDASScanOp scan_op(alloc);
  ObDASSearchCtx search_ctx(alloc, scan_op);
  class ChangingRangeOp : public TestEstimatedWorkSearchOp
  {
  public:
    explicit ChangingRangeOp(ObDASSearchCtx &ctx)
      : TestEstimatedWorkSearchOp(ctx, 0), available_(true), lo_(0), hi_(0), estimate_(0) {}
    int get_bitmap_input_range(bool &available, uint64_t &lo,
                               uint64_t &hi, int64_t &estimate) override
    {
      available = available_;
      lo = lo_;
      hi = hi_;
      estimate = estimate_;
      return OB_SUCCESS;
    }
    bool available_;
    uint64_t lo_;
    uint64_t hi_;
    int64_t estimate_;
  } source(search_ctx);
  ObFastBitmap bitmap(alloc);
  const int64_t table_estimate = 8841823;
  for (int round = 0; round < 3; ++round) {
    for (int scenario = 0; scenario < 5; ++scenario) {
      // Dense low/high domains, sparse domain, unknown domain, known empty.
      source.available_ = scenario != 3;
      source.lo_ = scenario == 1 ? (1ULL << 24) : 0;
      source.hi_ = source.lo_ + (scenario == 2 ? 4000000 : 127);
      source.estimate_ = scenario == 2 ? 2 : 128;
      if (scenario == 4) {
        source.lo_ = UINT64_MAX;
        source.hi_ = 0;
        source.estimate_ = 0;
      }
      ASSERT_EQ(OB_SUCCESS, bitmap.init_from_source(source, table_estimate));
      ASSERT_EQ(scenario < 2, bitmap.is_flat());
      ASSERT_EQ(0, bitmap.cardinality());
      ObFastBitmap::Iterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
      uint64_t id = 0;
      ASSERT_EQ(OB_ITER_END, iter.next_id(id));
      if (scenario >= 2) {
        ASSERT_EQ(135, bitmap.chunk_buf_count_);
      }
      if (scenario != 4) {
        const uint64_t ids[] = {source.lo_ + 1, source.hi_, source.lo_ + 1};
        ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(ids, 3));
        bitmap.finalize();
        ASSERT_EQ(2, bitmap.cardinality());
        ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
        ASSERT_EQ(OB_SUCCESS, iter.next_id(id));
        ASSERT_EQ(ids[0], id);
        ASSERT_EQ(OB_SUCCESS, iter.next_id(id));
        ASSERT_EQ(ids[1], id);
        ASSERT_EQ(OB_ITER_END, iter.next_id(id));
      }
    }
  }
}

TEST(ObFastBitmapFlatTest, failed_chunk_migration_retains_complete_flat_bitmap)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  class FailingChunkArrayAllocator : public ObMalloc
  {
  public:
    FailingChunkArrayAllocator()
      : ObMalloc(ObMemAttr(MTL_ID(), "FlatFailureTest")), fail_(true), attempts_(0) {}
    void *alloc(const int64_t size, const ObMemAttr &attr) override
    {
      ++attempts_;
      return fail_ ? nullptr : ObMalloc::alloc(size, attr);
    }
    bool fail_;
    int64_t attempts_;
  } chunk_allocator;
  ObArenaAllocator alloc("FlatFailureTest");
  ObFastBitmap bitmap(alloc);
  const uint64_t hi = 64 * ObFastBitmap::CHUNK_SIZE;
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(hi, true, 0, hi));
  const uint64_t ids[] = {1, hi};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(ids, 2));
  // Copy chunk 0 successfully, then fail expansion beyond the 32 inline slots.
  bitmap.chunks_.block_allocator_.allocator_ = &chunk_allocator;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, bitmap.add(hi + 1));
  ASSERT_EQ(1, chunk_allocator.attempts_);
  ASSERT_TRUE(bitmap.is_flat());
  ASSERT_EQ(0, bitmap.chunks_.count());
  bitmap.finalize();
  ASSERT_EQ(2, bitmap.cardinality());
  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  uint64_t id = 0;
  ASSERT_EQ(OB_SUCCESS, iter.next_id(id));
  ASSERT_EQ(ids[0], id);
  ASSERT_EQ(OB_SUCCESS, iter.next_id(id));
  ASSERT_EQ(ids[1], id);
  ASSERT_EQ(OB_ITER_END, iter.next_id(id));
  chunk_allocator.fail_ = false;
  ASSERT_EQ(OB_SUCCESS, bitmap.add(hi + 1));
  ASSERT_FALSE(bitmap.is_flat());
  ASSERT_EQ(3, bitmap.cardinality());
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  for (const uint64_t expected : {uint64_t(1), hi, hi + 1}) {
    ASSERT_EQ(OB_SUCCESS, iter.next_id(id));
    ASSERT_EQ(expected, id);
  }
  ASSERT_EQ(OB_ITER_END, iter.next_id(id));
}

TEST(ObFastBitmapFlatTest, admission_and_reinitialization)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  ObArenaAllocator alloc("FlatBitmapTest");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_INVALID_ARGUMENT, bitmap.init_with_range(-1, true, 0, 100));
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(100, false, 0, 100));
  ASSERT_FALSE(bitmap.is_flat());
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(1, true, 0, 10000));
  ASSERT_FALSE(bitmap.is_flat());
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(100, true, 0, UINT64_MAX));
  ASSERT_FALSE(bitmap.is_flat());
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(100, true, UINT64_MAX, 0));
  ASSERT_FALSE(bitmap.is_flat());
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(100, true, 100, 200));
  ASSERT_TRUE(bitmap.is_flat());
  ASSERT_EQ(OB_SUCCESS, bitmap.add(123));
  ASSERT_EQ(1, bitmap.cardinality());
  bitmap.reuse();
  ASSERT_EQ(0, bitmap.cardinality());
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(100, true, 10000, 10100));
  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  uint64_t value = 0;
  ASSERT_EQ(OB_ITER_END, iter.next_id(value));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(10001));
  ASSERT_EQ(OB_SUCCESS, iter.next_id(value));
  ASSERT_EQ(10001, value);
}

TEST(ObFastBitmapFlatTest, rescan_reuses_storage_with_arena_parent)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  ObArenaAllocator alloc("FlatBitmapTest");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(65536, false, 0, 0));
  auto *chunk_pool = bitmap.chunk_buf_;
  const int64_t parent_used = alloc.used();
  for (int round = 0; round < 100; ++round) {
    const uint64_t base = round * 65536ULL;
    ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(65536, true, base, base + 65535));
    auto *flat_words = bitmap.flat_words_;
    ASSERT_NE(nullptr, flat_words);
    ASSERT_EQ(OB_SUCCESS, bitmap.add(base + 65535));
    // Rebase a smaller range, then expand within the retained capacity.
    ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(64, true, base + 64, base + 127));
    ASSERT_EQ(flat_words, bitmap.flat_words_);
    ASSERT_EQ(OB_SUCCESS, bitmap.add(base + 64));
    ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(65536, true, base, base + 65535));
    ASSERT_EQ(flat_words, bitmap.flat_words_);
    ASSERT_EQ(0, bitmap.cardinality());
    ObFastBitmap::Iterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
    uint64_t id = 0;
    ASSERT_EQ(OB_ITER_END, iter.next_id(id));
    // Growth must replace owned storage, not accumulate in the parent arena.
    ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(131072, true, base, base + 131071));
    ASSERT_EQ(OB_SUCCESS, bitmap.add(base + 131071));
    ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(1, true, 0, 1000000));
    ASSERT_FALSE(bitmap.is_flat());
    ASSERT_EQ(0, bitmap.flat_allocator_.used());
    ASSERT_EQ(chunk_pool, bitmap.chunk_buf_);
    ASSERT_EQ(parent_used, alloc.used());
    ASSERT_EQ(OB_SUCCESS, bitmap.add(100));
    ASSERT_EQ(1, bitmap.cardinality());
  }
}

TEST(ObFastBitmapFlatTest, flat_fallback_preserves_chunk_allocator_tenant)
{
  struct TenantIdGuard
  {
    TenantIdGuard() : saved_id_(MTL_ID())
    { share::ObTenantEnv::get_tenant_local()->id_ = OB_SYS_TENANT_ID; }
    ~TenantIdGuard()
    { share::ObTenantEnv::get_tenant_local()->id_ = saved_id_; }
    uint64_t saved_id_;
  } tenant_guard;
  ASSERT_NE(OB_SERVER_TENANT_ID, MTL_ID());
  ObArenaAllocator alloc("FlatBitmapTest");
  ObFastBitmap bitmap(alloc);
  // Do not call init(): the first layout is Flat, with no chunk pool.
  const uint64_t base = 64 * ObFastBitmap::CHUNK_SIZE;
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(64, true, base, base + 63));
  ASSERT_TRUE(bitmap.is_flat());
  ASSERT_EQ(nullptr, bitmap.chunk_buf_);
  EXPECT_EQ(MTL_ID(), bitmap.chunks_.block_allocator_.get_attr().tenant_id_);
  EXPECT_TRUE(bitmap.chunks_.block_allocator_.get_label() == "FBitmapChunks");
  ASSERT_EQ(OB_SUCCESS, bitmap.add(base + 1));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(base + 63));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(base + 64));
  ASSERT_FALSE(bitmap.is_flat());
  ASSERT_GT(bitmap.chunks_.count(), 32);
  EXPECT_EQ(MTL_ID(), bitmap.chunks_.block_allocator_.get_attr().tenant_id_);
  EXPECT_TRUE(bitmap.chunks_.block_allocator_.get_label() == "FBitmapChunks");
  ASSERT_EQ(3, bitmap.cardinality());
  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  uint64_t id = 0;
  for (const uint64_t expected : {base + 1, base + 63, base + 64}) {
    ASSERT_EQ(OB_SUCCESS, iter.next_id(id));
    ASSERT_EQ(expected, id);
  }
  ASSERT_EQ(OB_ITER_END, iter.next_id(id));
}

TEST(ObFastBitmapFlatTest, unordered_batches_and_window_oracle)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  ObArenaAllocator alloc("FlatBitmapTest");
  ObFastBitmap flat(alloc);
  ObFastBitmap chunked(alloc);
  ASSERT_EQ(OB_SUCCESS, flat.init_with_range(20000, true, 63, 131100));
  ASSERT_TRUE(flat.is_flat());
  ASSERT_EQ(OB_SUCCESS, chunked.init(20000));
  std::vector<uint64_t> values;
  for (uint64_t i = 0; i < 4096; ++i) {
    values.push_back(63 + ((i * 31337) % 131038));
  }
  for (const uint64_t id : {63ULL, 64ULL, 65535ULL, 65536ULL, 131100ULL, 64ULL}) {
    values.push_back(id);
  }
  for (size_t i = 0; i < values.size(); i += 256) {
    const int64_t count = std::min<size_t>(256, values.size() - i);
    ASSERT_EQ(OB_SUCCESS, flat.add_batch(values.data() + i, count));
    ASSERT_EQ(OB_SUCCESS, chunked.add_batch(values.data() + i, count));
  }
  flat.finalize();
  chunked.finalize();
  std::sort(values.begin(), values.end());
  values.erase(std::unique(values.begin(), values.end()), values.end());
  ASSERT_EQ(values.size(), flat.cardinality());
  ASSERT_EQ(chunked.cardinality(), flat.cardinality());
  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&flat));
  uint64_t actual = 0;
  for (const uint64_t expected : values) {
    ASSERT_EQ(OB_SUCCESS, iter.next_id(actual));
    ASSERT_EQ(expected, actual);
  }
  ASSERT_EQ(OB_ITER_END, iter.next_id(actual));
  for (uint64_t start = 65500; start < 65564; ++start) {
    uint64_t a[5] = {};
    uint64_t b[5] = {};
    int64_t ac = 0;
    int64_t bc = 0;
    ASSERT_EQ(OB_SUCCESS, flat.copy_range_to_words(start, start + 256, start, 257, a, ac));
    ASSERT_EQ(OB_SUCCESS, chunked.copy_range_to_words(start, start + 256, start, 257, b, bc));
    ASSERT_EQ(ac, bc);
    for (int i = 0; i < 5; ++i) { ASSERT_EQ(a[i], b[i]); }
  }
}

TEST(ObFastBitmapFlatTest, high_base_drain_and_fallback)
{
  share::ObTenantEnv::get_tenant_local()->id_ = OB_SERVER_TENANT_ID;
  ObArenaAllocator alloc("FlatBitmapTest");
  ObFastBitmap bitmap(alloc);
  const uint64_t base = 1ULL << 50;
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(64, true, base + 1, base + 100));
  ASSERT_TRUE(bitmap.is_flat());
  const uint64_t ids[] = {base + 1, base + 63, base + 64, base + 100};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(ids, 4));
  bitmap.finalize();
  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  uint64_t out[8] = {};
  int64_t count = 0;
  uint64_t after = 0;
  ObFastBitmap::Iterator::DrainState state;
  ASSERT_EQ(OB_SUCCESS, iter.drain_until(base + 63, 8, out, count, after, state));
  ASSERT_EQ(2, count);
  ASSERT_EQ(ids[0], out[0]);
  ASSERT_EQ(ids[1], out[1]);
  ASSERT_EQ(ids[2], after);
  ASSERT_EQ(ObFastBitmap::Iterator::DRAIN_BOUNDARY_REACHED, state);
  ASSERT_EQ(OB_SUCCESS, iter.drain_until(UINT64_MAX, 1, out, count, after, state));
  ASSERT_EQ(ids[3], out[0]);
  ASSERT_EQ(ObFastBitmap::Iterator::DRAIN_BATCH_FULL, state);
  ASSERT_EQ(OB_SUCCESS, iter.drain_until(UINT64_MAX, 1, out, count, after, state));
  ASSERT_EQ(0, count);
  ASSERT_EQ(ObFastBitmap::Iterator::DRAIN_ITER_END, state);
  // Exercise migration at a practical chunk-array address, including dirty data.
  ASSERT_EQ(OB_SUCCESS, bitmap.init_with_range(64, true, 63, 100));
  const uint64_t initial[] = {63, 64, 100, 64};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(initial, 4));
  const uint64_t outside[] = {0, 65536, 64};
  ASSERT_EQ(OB_SUCCESS, bitmap.add_batch(outside, 3));
  ASSERT_FALSE(bitmap.is_flat());
  bitmap.finalize();
  ASSERT_EQ(5, bitmap.cardinality());
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  for (uint64_t expected : {0ULL, 63ULL, 64ULL, 100ULL, 65536ULL}) {
    ASSERT_EQ(OB_SUCCESS, iter.next_id(after));
    ASSERT_EQ(expected, after);
  }
  ASSERT_EQ(OB_ITER_END, iter.next_id(after));
}

} // anonymous namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

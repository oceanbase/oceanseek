/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <algorithm>
#include <vector>

#include "lib/allocator/page_arena.h"
#include "lib/worker.h"
#include "lib/utility/ob_defer.h"
#include "common/ob_tenant_data_version_mgr.h"
#include "share/rc/ob_tenant_base.h"
#define private public
#define protected public
#include "sql/das/iter/ob_das_iter_utils.h"
#include "sql/das/iter/ob_das_ivf_scan_iter.h"
#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/iter/ob_das_profile_iter.h"
#include "sql/das/iter/ob_das_vec_index_ivf_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_async_ivf_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "share/diagnosis/ob_profile_util.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/access/ob_table_scan_range.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable.h"
#undef protected
#undef private
#include "storage/blocksstable/vector_index/ob_ivf_sstable_scan_iter.h"

namespace oceanbase
{
namespace sql
{

using namespace common;
using namespace share;

class AsyncIVFFailAllocator : public ObIAllocator
{
public:
  AsyncIVFFailAllocator(ObIAllocator &allocator, const int64_t fail_at)
    : allocator_(allocator), fail_at_(fail_at), attempts_(0)
  {}

  void *alloc(const int64_t size) override
  {
    void *ptr = nullptr;
    if (++attempts_ != fail_at_) {
      ptr = allocator_.alloc(size);
      if (nullptr != ptr) {
        allocations_.push_back(ptr);
      }
    }
    return ptr;
  }

  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }

  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      const auto pos = std::find(allocations_.begin(), allocations_.end(), ptr);
      EXPECT_NE(allocations_.end(), pos);
      if (allocations_.end() != pos) {
        allocator_.free(ptr);
        allocations_.erase(pos);
      }
    }
  }

  ObIAllocator &allocator_;
  int64_t fail_at_;
  int64_t attempts_;
  std::vector<void *> allocations_;
};

class MockAsyncIVFLookupScanIter : public ObDASScanIter
{
public:
  MockAsyncIVFLookupScanIter()
    : scan_count_(0), rescan_count_(0), observed_snapshot_(0), rescan_ret_(OB_SUCCESS)
  {}

  int do_table_scan() override
  {
    int ret = OB_SUCCESS;
    ++scan_count_;
    observed_snapshot_ = get_scan_param().snapshot_.version().get_val_for_tx();
    // Match storage's first-open contract: switch_param() requires an initialized iterator.
    if (nullptr == get_output_result_iter() && get_scan_param().need_switch_param_) {
      ret = OB_NOT_INIT;
    }
    return ret;
  }

  int rescan() override
  {
    ++rescan_count_;
    observed_snapshot_ = get_scan_param().snapshot_.version().get_val_for_tx();
    return rescan_ret_;
  }

  int64_t scan_count_;
  int64_t rescan_count_;
  int64_t observed_snapshot_;
  int rescan_ret_;
};

class MockAsyncIVFPreFilterScanIter : public ObDASScanIter
{
public:
  MockAsyncIVFPreFilterScanIter(ObEvalCtx &eval_ctx, ObExpr &rowid_expr,
                                const uint64_t *rowids, const int64_t count,
                                const int64_t fail_at)
    : rowid_expr_(rowid_expr), rowids_(rowids), count_(count), pos_(0), fail_at_(fail_at),
      fail_ret_(OB_TIMEOUT), read_calls_(0)
  {
    eval_ctx_ = &eval_ctx;
    inited_ = true;
    max_size_ = 1;
  }

  void clear_evaluated_flag() override {}

  int inner_get_next_row() override
  {
    ++read_calls_;
    int ret = OB_SUCCESS;
    if (pos_ == fail_at_) {
      ret = fail_ret_;
    } else if (pos_ == count_) {
      ret = OB_ITER_END;
    } else {
      rowid_expr_.locate_datum_for_write(*eval_ctx_).set_uint(rowids_[pos_++]);
    }
    return ret;
  }

  int inner_get_next_rows(int64_t &count, int64_t capacity) override
  {
    UNUSED(capacity);
    int ret = inner_get_next_row();
    count = OB_SUCC(ret) ? 1 : 0;
    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      // A failed batch may leave output buffers/count populated; these rows
      // have not completed filtering and must never enter the whitelist.
      count = 1;
      rowid_expr_.locate_datum_for_write(*eval_ctx_).set_uint(UINT64_MAX);
    }
    if (OB_SUCC(ret) && pos_ == count_) {
      // Exercise a final valid batch returned together with OB_ITER_END.
      ret = OB_ITER_END;
    }
    return ret;
  }

  ObExpr &rowid_expr_;
  const uint64_t *rowids_;
  int64_t count_;
  int64_t pos_;
  int64_t fail_at_;
  int fail_ret_;
  int64_t read_calls_;
};

class MockAsyncIVFPostFilterScanIter : public ObDASScanIter
{
public:
  MockAsyncIVFPostFilterScanIter()
    : scan_count_(0), rescan_count_(0), last_key_count_(0), rescan_ret_(OB_SUCCESS), reuse_count_(0)
  {
    inited_ = true;
  }

  int do_table_scan() override
  {
    ++scan_count_;
    last_key_count_ = get_scan_param().key_ranges_.count();
    return OB_SUCCESS;
  }

  int rescan() override
  {
    ++rescan_count_;
    last_key_count_ = get_scan_param().key_ranges_.count();
    return rescan_ret_;
  }

  void clear_evaluated_flag() override {}
  int inner_get_next_row() override { return OB_ITER_END; }
  int inner_get_next_rows(int64_t &count, int64_t capacity) override
  {
    UNUSED(capacity);
    count = 0;
    return OB_ITER_END;
  }
  int inner_reuse() override
  {
    ++reuse_count_;
    get_scan_param().key_ranges_.reuse();
    return OB_SUCCESS;
  }

  int64_t scan_count_;
  int64_t rescan_count_;
  int64_t last_key_count_;
  int rescan_ret_;
  int64_t reuse_count_;
};

// Use real storage datum-rowkey materialization/reset, while controlling scan
// failures without a running storage service.
class MockAsyncIVFAllocatingPostFilterScanIter : public MockAsyncIVFPostFilterScanIter
{
public:
  int do_table_scan() override
  {
    MockAsyncIVFPostFilterScanIter::do_table_scan();
    return materialize();
  }
  int rescan() override
  {
    MockAsyncIVFPostFilterScanIter::rescan();
    return materialize();
  }
  int materialize()
  {
    range_.allocator_ = get_scan_param().scan_allocator_;
    storage::ObTablet tablet;
    ObQueryFlag scan_flag;
    int ret = range_.init_rowkeys(tablet, false, get_scan_param().key_ranges_, scan_flag, nullptr);
    if (OB_SUCCESS == ret) {
      // Model allocations abandoned by common scan code, including ColumnMap.
      abandoned_ = get_scan_param().scan_allocator_->alloc(300);
      if (nullptr == abandoned_) { ret = OB_ALLOCATE_MEMORY_FAILED; }
      else { MEMSET(abandoned_, 0x5a, 300); }
    }
    return OB_SUCCESS == ret ? open_ret_ : ret;
  }
  int inner_get_next_row() override { return read_ret_; }
  int inner_get_next_rows(int64_t &count, int64_t capacity) override
  {
    UNUSED(capacity);
    count = 0;
    return read_ret_;
  }
  int inner_reuse() override
  {
    if (OB_SUCCESS != reuse_ret_) {
      ++reuse_count_;
      return reuse_ret_; // A failed cleanup may still borrow every datum.
    }
    range_.reset();
    return MockAsyncIVFPostFilterScanIter::inner_reuse();
  }
  storage::ObTableScanRange range_;
  void *abandoned_ = nullptr;
  int open_ret_ = OB_SUCCESS;
  int read_ret_ = OB_ITER_END;
  int reuse_ret_ = OB_SUCCESS;
};

class TestDASAsyncIVFScanIter : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ObTenantEnv::set_tenant(&tenant_base_);
  }

  void TearDown() override
  {
    ObTenantEnv::set_tenant(nullptr);
  }

  void init_expr_frame(ObExpr &expr,
                       const ObObjType type,
                       const ObObjDatumMapType datum_map,
                       const int64_t result_length,
                       int64_t &frame_pos)
  {
    expr.obj_meta_.set_type(type);
    expr.obj_meta_.set_collation_type(CS_TYPE_BINARY);
    expr.datum_meta_.type_ = type;
    expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
    expr.obj_datum_map_ = datum_map;
    expr.frame_idx_ = 0;
    expr.datum_off_ = frame_pos;
    frame_pos += sizeof(ObDatum);
    expr.eval_info_off_ = frame_pos;
    frame_pos += sizeof(ObEvalInfo);
    expr.res_buf_off_ = frame_pos;
    expr.res_buf_len_ = result_length;
    frame_pos += result_length;
  }

  int init_vector_lookup_exprs(ObEvalCtx &eval_ctx,
                               const float *query_vector,
                               const int64_t query_vector_size,
                               ObExpr &rowkey_expr,
                               ObExpr &query_expr,
                               ObExpr &limit_expr,
                               ObExpr &distance_expr)
  {
    int ret = OB_SUCCESS;
    const int64_t frame_size = 4096;
    char **frames = static_cast<char **>(allocator_.alloc(sizeof(char *)));
    if (OB_ISNULL(frames)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(frames[0] = static_cast<char *>(allocator_.alloc(frame_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(frames[0], 0, frame_size);
      eval_ctx.frames_ = frames;
      int64_t frame_pos = 0;
      init_expr_frame(rowkey_expr, ObUInt64Type, OBJ_DATUM_8BYTE_DATA,
                      sizeof(uint64_t), frame_pos);
      init_expr_frame(query_expr, ObVarcharType, OBJ_DATUM_STRING,
                      query_vector_size, frame_pos);
      init_expr_frame(limit_expr, ObIntType, OBJ_DATUM_8BYTE_DATA,
                      sizeof(int64_t), frame_pos);
      if (OB_UNLIKELY(frame_pos >= frame_size)) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        query_expr.is_static_const_ = true;
        query_expr.locate_datum_for_write(eval_ctx).set_string(
            reinterpret_cast<const char *>(query_vector), query_vector_size);
        query_expr.set_evaluated_projected(eval_ctx);
        limit_expr.is_static_const_ = true;
        limit_expr.locate_datum_for_write(eval_ctx).set_int(2);
        limit_expr.set_evaluated_projected(eval_ctx);
        ObExpr **distance_args = static_cast<ObExpr **>(allocator_.alloc(2 * sizeof(ObExpr *)));
        if (OB_ISNULL(distance_args)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          distance_args[0] = &rowkey_expr;
          distance_args[1] = &query_expr;
          distance_expr.type_ = T_FUN_SYS_L2_DISTANCE;
          distance_expr.args_ = distance_args;
          distance_expr.arg_cnt_ = 2;
        }
      }
    }
    return ret;
  }

  void verify_runtime_tree(const ObVecIndexType vec_type,
                            const bool needs_residual_lookup = false,
                            const bool primary_scan = false,
                            const ObVecIdxAdaTryPath try_path = ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN,
                            const int expected_ret = OB_SUCCESS,
                            const ObVecIdxQueryStrategy strategy = ObVecIdxQueryStrategy::RECALL_FIRST)
  {
    const int64_t async_ivf_child_count = 3;
    const uint64_t data_table_id = 50001;
    const uint64_t async_ivf_index_tid = 60001;
    const uint64_t async_delete_index_tid = 60002;
    float query_vector[2] = {1.0F, 0.0F};
    ObExecContext exec_ctx(allocator_);
    ObSQLSessionInfo session;
    ObEvalCtx eval_ctx(exec_ctx);
    ObExpr rowkey_expr;
    ObExpr query_expr;
    ObExpr limit_expr;
    ObExpr distance_expr;
    ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
    exec_ctx.set_my_session(&session);
    exec_ctx.get_physical_plan_ctx()->set_timeout_timestamp(INT64_MAX);
    ASSERT_EQ(OB_SUCCESS,
              init_vector_lookup_exprs(eval_ctx, query_vector, sizeof(query_vector),
                                       rowkey_expr, query_expr, limit_expr, distance_expr));

    ObDASTableLookupCtDef lookup_ctdef(allocator_);
    ObDASSortCtDef sort_ctdef(allocator_);
    ObDASVecAuxScanCtDef vec_scan_ctdef(allocator_);
    ObDASScanCtDef filter_input_ctdef(allocator_);
    ObDASScanCtDef ivf_scan_ctdef(allocator_);
    ObDASScanCtDef delete_scan_ctdef(allocator_);
    ObDASScanCtDef lookup_scan_ctdef(allocator_);
    ObDASTableLookupRtDef lookup_rtdef;
    ObDASSortRtDef sort_rtdef;
    ObDASVecAuxScanRtDef vec_scan_rtdef;
    ObDASScanRtDef filter_input_rtdef;
    ObDASScanRtDef ivf_scan_rtdef;
    ObDASScanRtDef delete_scan_rtdef;
    ObDASScanRtDef lookup_scan_rtdef;
    ObDASBaseCtDef *lookup_ctdef_children[2] = {&sort_ctdef, &lookup_scan_ctdef};
    ObDASBaseCtDef *sort_ctdef_children[1] = {&vec_scan_ctdef};
    ObDASBaseCtDef *vec_ctdef_children[async_ivf_child_count] = {
        &filter_input_ctdef, &ivf_scan_ctdef, &delete_scan_ctdef};
    ObDASBaseRtDef *lookup_rtdef_children[2] = {&sort_rtdef, &lookup_scan_rtdef};
    ObDASBaseRtDef *sort_rtdef_children[1] = {&vec_scan_rtdef};
    ObDASBaseRtDef *vec_rtdef_children[async_ivf_child_count] = {
        &filter_input_rtdef, &ivf_scan_rtdef, &delete_scan_rtdef};

    lookup_ctdef.children_ = lookup_ctdef_children;
    lookup_ctdef.children_cnt_ = ARRAYSIZEOF(lookup_ctdef_children);
    sort_ctdef.children_ = sort_ctdef_children;
    sort_ctdef.children_cnt_ = ARRAYSIZEOF(sort_ctdef_children);
    vec_scan_ctdef.children_ = vec_ctdef_children;
    vec_scan_ctdef.children_cnt_ = ARRAYSIZEOF(vec_ctdef_children);
    lookup_rtdef.children_ = lookup_rtdef_children;
    lookup_rtdef.children_cnt_ = ARRAYSIZEOF(lookup_rtdef_children);
    sort_rtdef.children_ = sort_rtdef_children;
    sort_rtdef.children_cnt_ = ARRAYSIZEOF(sort_rtdef_children);
    vec_scan_rtdef.children_ = vec_rtdef_children;
    vec_scan_rtdef.children_cnt_ = ARRAYSIZEOF(vec_rtdef_children);

    ObDASBaseRtDef *rtdefs[] = {
        &lookup_rtdef, &sort_rtdef, &vec_scan_rtdef, &filter_input_rtdef,
        &ivf_scan_rtdef, &delete_scan_rtdef, &lookup_scan_rtdef};
    const ObDASBaseCtDef *ctdefs[] = {
        &lookup_ctdef, &sort_ctdef, &vec_scan_ctdef, &filter_input_ctdef,
        &ivf_scan_ctdef, &delete_scan_ctdef, &lookup_scan_ctdef};
    for (int64_t i = 0; i < ARRAYSIZEOF(rtdefs); ++i) {
      rtdefs[i]->ctdef_ = ctdefs[i];
      rtdefs[i]->eval_ctx_ = &eval_ctx;
    }

    ASSERT_EQ(OB_SUCCESS, sort_ctdef.sort_exprs_.init(1));
    ASSERT_EQ(OB_SUCCESS, vec_scan_ctdef.result_output_.init(1));
    ASSERT_EQ(OB_SUCCESS, ivf_scan_ctdef.rowkey_exprs_.init(1));
    ASSERT_EQ(OB_SUCCESS, lookup_scan_ctdef.rowkey_exprs_.init(1));
    ASSERT_EQ(OB_SUCCESS, lookup_scan_ctdef.result_output_.init(1));
    ASSERT_EQ(OB_SUCCESS, sort_ctdef.sort_exprs_.push_back(&distance_expr));
    sort_ctdef.limit_expr_ = &limit_expr;
    ASSERT_EQ(OB_SUCCESS, vec_scan_ctdef.result_output_.push_back(&rowkey_expr));
    ASSERT_EQ(OB_SUCCESS, ivf_scan_ctdef.rowkey_exprs_.push_back(&rowkey_expr));
    ASSERT_EQ(OB_SUCCESS, lookup_scan_ctdef.rowkey_exprs_.push_back(&rowkey_expr));
    ASSERT_EQ(OB_SUCCESS, lookup_scan_ctdef.result_output_.push_back(&rowkey_expr));
    vec_scan_ctdef.is_async_index_ = true;
    vec_scan_ctdef.algorithm_type_ = ObVectorIndexAlgorithmType::VIAT_DISK_BBQ;
    vec_scan_ctdef.vec_type_ = vec_type;
    vec_scan_ctdef.adaptive_try_path_ = try_path;
    vec_scan_ctdef.strategy_ = strategy;
    vec_scan_ctdef.pre_filtering_timeout_ = 10000;
    vec_scan_ctdef.vec_index_param_ = ObString("TYPE=DISK_BBQ,DISTANCE=L2");
    vec_scan_ctdef.vector_index_param_.type_ = ObVectorIndexAlgorithmType::VIAT_DISK_BBQ;
    vec_scan_ctdef.vector_index_param_.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_L2;
    vec_scan_ctdef.vec_query_param_.is_set_ivf_nprobes_ = true;
    vec_scan_ctdef.vec_query_param_.ivf_nprobes_ = 1;
    vec_scan_ctdef.dim_ = ARRAYSIZEOF(query_vector);
    ivf_scan_ctdef.ref_table_id_ = async_ivf_index_tid;
    ivf_scan_ctdef.ir_scan_type_ = ObTSCIRScanType::OB_VEC_ASYNC_IVF_INDEX_SCAN;
    delete_scan_ctdef.ref_table_id_ = async_delete_index_tid;
    delete_scan_ctdef.ir_scan_type_ = ObTSCIRScanType::OB_ASYNC_INDEX_DELETE_SCAN;
    lookup_scan_ctdef.ref_table_id_ = data_table_id;
    lookup_scan_ctdef.table_param_.main_read_info_.schema_rowkey_cnt_ = 1;
    lookup_scan_rtdef.scan_flag_.is_lookup_for_4377_ = true;
    const bool vector_scan_input = ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER == vec_type
        || ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER == vec_type;
    filter_input_ctdef.ref_table_id_ = vector_scan_input
        ? async_ivf_index_tid : (primary_scan ? data_table_id : 60003);
    filter_input_ctdef.ir_scan_type_ = vector_scan_input
        ? ObTSCIRScanType::OB_IR_INV_IDX_SCAN : ObTSCIRScanType::OB_VEC_FILTER_SCAN;
    if (needs_residual_lookup) {
      ASSERT_EQ(OB_SUCCESS, lookup_scan_ctdef.pd_expr_spec_.pushdown_filters_.init(1));
      ASSERT_EQ(OB_SUCCESS, lookup_scan_ctdef.pd_expr_spec_.pushdown_filters_.push_back(&rowkey_expr));
    }
    ASSERT_EQ(OB_SUCCESS, lookup_scan_rtdef.init_pd_op(exec_ctx, lookup_scan_ctdef));
    ASSERT_EQ(OB_SUCCESS, filter_input_rtdef.init_pd_op(exec_ctx, filter_input_ctdef));

    ObDASRelatedTabletID related_tablet_ids(allocator_);
    related_tablet_ids.lookup_tablet_id_ = ObTabletID(100001);
    related_tablet_ids.async_ivf_index_tablet_id_ = ObTabletID(200001);
    related_tablet_ids.async_delete_tablet_id_ = ObTabletID(200002);
    ObTableScanParam scan_param;
    scan_param.ls_id_ = ObLSID(1);
    scan_param.index_id_ = filter_input_ctdef.ref_table_id_;
    scan_param.tablet_id_ = primary_scan ? related_tablet_ids.lookup_tablet_id_ : ObTabletID(200003);
    scan_param.op_ = filter_input_rtdef.p_pd_expr_op_;
    ObNewRange input_range;
    input_range.table_id_ = scan_param.index_id_;
    input_range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, scan_param.key_ranges_.push_back(input_range));
    transaction::ObTxDesc tx_desc;
    transaction::ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, snapshot.core_.version_.convert_for_tx(1000));
    ObDASIter *iter_tree = nullptr;

    ASSERT_EQ(expected_ret,
              ObDASIterUtils::create_das_scan_iter_tree(
                  ObDASIterTreeType::ITER_TREE_VEC_LOOKUP, scan_param,
                  nullptr, nullptr, nullptr, nullptr, &lookup_ctdef,
                  &lookup_rtdef, related_tablet_ids, &tx_desc, &snapshot,
                  allocator_, nullptr, iter_tree));
    if (OB_SUCCESS != expected_ret) {
      EXPECT_EQ(nullptr, iter_tree);
    } else {
      ASSERT_NE(nullptr, iter_tree);
      ASSERT_EQ(ObDASIterType::DAS_ITER_LOCAL_LOOKUP, iter_tree->get_type());
      ASSERT_EQ(2, iter_tree->get_children_cnt());
      ASSERT_NE(nullptr, iter_tree->get_children());
      EXPECT_EQ(ObDASIterType::DAS_ITER_ASYNC_IVF_SCAN,
                iter_tree->get_children()[0]->get_type());
      EXPECT_EQ(ObDASIterType::DAS_ITER_SCAN,
                iter_tree->get_children()[1]->get_type());
      ObDASIVFAsyncIter *async_ivf_iter =
          static_cast<ObDASIVFAsyncIter *>(iter_tree->get_children()[0]);
      ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter *>(iter_tree);
      EXPECT_EQ(&snapshot, async_ivf_iter->sql_snapshot_);
      EXPECT_EQ(async_ivf_iter->get_async_read_snapshot(), lookup_iter->snapshot_);
      EXPECT_EQ(try_path, async_ivf_iter->vec_idx_try_path_);
      ObDASIter *filter_iter = async_ivf_iter->get_inv_idx_scan_iter();
      ASSERT_NE(nullptr, filter_iter);
      ASSERT_EQ(ObDASIterType::DAS_ITER_LOCAL_LOOKUP, filter_iter->get_type());
      ObDASLocalLookupIter *pre_lookup = static_cast<ObDASLocalLookupIter *>(filter_iter);
      EXPECT_EQ(async_ivf_iter->get_async_read_snapshot(), pre_lookup->snapshot_);
      EXPECT_EQ(&lookup_scan_ctdef, pre_lookup->get_lookup_ctdef());
      EXPECT_NE(pre_lookup->get_data_table_iter(), iter_tree->get_children()[1]);
      filter_iter = pre_lookup->get_index_table_iter();
      ASSERT_NE(nullptr, filter_iter);
      ASSERT_EQ(ObDASIterType::DAS_ITER_SCAN, filter_iter->get_type());
      ObDASScanIter *filter_scan = static_cast<ObDASScanIter *>(filter_iter);
      EXPECT_EQ(strategy, async_ivf_iter->strategy_);
      EXPECT_EQ(strategy == ObVecIdxQueryStrategy::LATENCY_FIRST,
                filter_scan->is_vec_pre_filtering_timeout_set());
      EXPECT_EQ(&filter_input_ctdef, filter_scan->scan_ctdef_);
      EXPECT_EQ(&scan_param, &filter_scan->get_scan_param());
      ASSERT_NE(nullptr, async_ivf_iter->data_filter_iter_);
      EXPECT_EQ(&lookup_scan_ctdef, async_ivf_iter->data_filter_ctdef_);
      EXPECT_EQ(&lookup_scan_rtdef, async_ivf_iter->data_filter_rtdef_);
      EXPECT_EQ(&async_ivf_iter->data_filter_scan_param_,
                &async_ivf_iter->data_filter_iter_->get_scan_param());
      EXPECT_NE(static_cast<ObDASIter *>(async_ivf_iter->data_filter_iter_),
                iter_tree->get_children()[1]);
      EXPECT_NE(static_cast<ObDASIter *>(async_ivf_iter->data_filter_iter_),
                pre_lookup->get_data_table_iter());
      EXPECT_NE(&filter_scan->get_scan_param(), &pre_lookup->get_lookup_param());
      EXPECT_NE(&filter_scan->get_scan_param(), &async_ivf_iter->data_filter_scan_param_);
      SCN published_scn;
      ASSERT_EQ(OB_SUCCESS, published_scn.convert_for_tx(800));
      ASSERT_EQ(OB_SUCCESS, async_ivf_iter->prepare_async_read_snapshot(published_scn));
      ASSERT_EQ(OB_SUCCESS, async_ivf_iter->prepare_filter_input_scan_params());
      EXPECT_EQ(published_scn, filter_scan->get_scan_param().snapshot_.version());
      EXPECT_EQ(published_scn, pre_lookup->get_lookup_param().snapshot_.version());
      EXPECT_EQ(1000, snapshot.version().get_val_for_tx());

      // DAS rebinds the original input; Async IVF prepares its read context.
      scan_param.ls_id_ = ObLSID(2);
      scan_param.tablet_id_ = ObTabletID(200003);
      related_tablet_ids.lookup_tablet_id_ = ObTabletID(100002);
      async_ivf_iter->set_ls_tablet_ids(ObLSID(2), related_tablet_ids);
      ASSERT_EQ(OB_SUCCESS, published_scn.convert_for_tx(900));
      ASSERT_EQ(OB_SUCCESS, async_ivf_iter->prepare_async_read_snapshot(published_scn));
      ASSERT_EQ(OB_SUCCESS, async_ivf_iter->prepare_filter_input_scan_params());
      EXPECT_EQ(published_scn, filter_scan->get_scan_param().snapshot_.version());
      EXPECT_EQ(published_scn, pre_lookup->get_lookup_param().snapshot_.version());
      ASSERT_EQ(1, scan_param.key_ranges_.count());
      EXPECT_EQ(input_range.table_id_, scan_param.key_ranges_.at(0).table_id_);
      EXPECT_EQ(filter_input_ctdef.ref_table_id_, filter_scan->get_scan_param().index_id_);
      EXPECT_EQ(filter_input_rtdef.p_pd_expr_op_, filter_scan->get_scan_param().op_);
      EXPECT_EQ(ObLSID(2), filter_scan->get_scan_param().ls_id_);
      EXPECT_EQ(ObTabletID(200003), filter_scan->get_scan_param().tablet_id_);
      EXPECT_EQ(OB_SUCCESS, iter_tree->release());
    }
  }

  void verify_pre_filter_materialization(const bool is_vectorized,
                                           const uint64_t *rowids,
                                           const int64_t row_count,
                                           const int64_t fail_at,
                                           const int expected_ret,
                                           const int64_t expected_count,
                                           const int fail_ret = OB_TIMEOUT,
                                           const ObVecIdxQueryStrategy strategy = ObVecIdxQueryStrategy::RECALL_FIRST,
                                           const int64_t batch_size = 1,
                                           const bool verify_entry_budget = false)
  {
    float query_vector[2] = {1.0F, 0.0F};
    ObExecContext exec_ctx(allocator_);
    ObEvalCtx eval_ctx(exec_ctx);
    ObExpr rowid_expr;
    ObExpr query_expr;
    ObExpr limit_expr;
    ObExpr distance_expr;
    ASSERT_EQ(OB_SUCCESS, init_vector_lookup_exprs(eval_ctx, query_vector, sizeof(query_vector),
        rowid_expr, query_expr, limit_expr, distance_expr));
    ObDASVecAuxScanCtDef ctdef(allocator_);
    ObDASVecAuxScanRtDef rtdef;
    ObDASScanCtDef index_ctdef(allocator_);
    ObDASScanRtDef index_rtdef;
    ObDASBaseCtDef *ctdef_children[] = {nullptr, &index_ctdef, nullptr};
    ObDASBaseRtDef *rtdef_children[] = {nullptr, &index_rtdef, nullptr};
    ctdef.children_ = ctdef_children;
    ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
    ctdef.is_async_index_ = true;
    rtdef.children_ = rtdef_children;
    rtdef.children_cnt_ = ARRAYSIZEOF(rtdef_children);
    rtdef.ctdef_ = &ctdef;
    index_ctdef.ir_scan_type_ = ObTSCIRScanType::OB_VEC_ASYNC_IVF_INDEX_SCAN;
    ASSERT_EQ(OB_SUCCESS, index_ctdef.rowkey_exprs_.init(1));
    ASSERT_EQ(OB_SUCCESS, index_ctdef.rowkey_exprs_.push_back(&rowid_expr));
    index_rtdef.ctdef_ = &index_ctdef;
    index_rtdef.eval_ctx_ = &eval_ctx;
    ASSERT_EQ(OB_SUCCESS, ctdef.result_output_.init(1));
    ASSERT_EQ(OB_SUCCESS, ctdef.result_output_.push_back(&rowid_expr));
    rtdef.eval_ctx_ = &eval_ctx;
    MockAsyncIVFPreFilterScanIter scan(eval_ctx, rowid_expr, rowids, row_count, fail_at);
    scan.fail_ret_ = fail_ret;
    scan.max_size_ = batch_size;
    ObDASIVFAsyncIter iter;
    iter.strategy_ = strategy;
    blocksstable::ObIvfPreFilterBitmap pre_filter_bitmap(allocator_);
    iter.vec_aux_ctdef_ = &ctdef;
    iter.vec_aux_rtdef_ = &rtdef;
    iter.inv_idx_scan_iter_ = &scan;
    ObRoaringBitmap *deleted = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_);
    ASSERT_NE(nullptr, deleted);
    ASSERT_EQ(OB_SUCCESS, deleted->value_add(3));
    ASSERT_EQ(OB_SUCCESS, iter.deletion_bitmap_handle_.init_from_bitmap(allocator_, deleted));

    ASSERT_EQ(expected_ret, iter.materialize_pre_filter(is_vectorized, pre_filter_bitmap));
    const bool input_complete = expected_ret == OB_SUCCESS && (fail_at < 0 || fail_at >= row_count);
    EXPECT_EQ(expected_count, iter.adaptive_ctx_.pre_scan_row_cnt_);
    EXPECT_EQ(expected_count, pre_filter_bitmap.get_cardinality());
    if (expected_count == 0) {
      EXPECT_TRUE(pre_filter_bitmap.get_roaring()->is_empty_type());
    } else if (expected_count == 1) {
      EXPECT_TRUE(pre_filter_bitmap.get_roaring()->is_single_type());
    } else if (expected_count <= MAX_BITMAP_SET_VALUES) {
      EXPECT_TRUE(pre_filter_bitmap.get_roaring()->is_set_type());
    } else {
      EXPECT_TRUE(pre_filter_bitmap.is_compact() || pre_filter_bitmap.is_roaring());
    }
    if (expected_count > 0) {
      const int64_t valid_rows = fail_at < 0 ? row_count : fail_at;
      for (int64_t i = 0; i < valid_rows; ++i) {
        EXPECT_TRUE(pre_filter_bitmap.is_contains(rowids[i]));
      }
    }
    if (!input_complete) {
      blocksstable::ObIvfKnnSearchResult retained;
      blocksstable::ObIvfKnnEntry retained_entry;
      retained_entry.cid_ = 7;
      retained_entry.vid_ = row_count + 100;
      retained_entry.distance_ = 0.25F;
      ASSERT_EQ(OB_SUCCESS, retained.add(retained_entry));
      iter.limit_param_.limit_ = 1;
      iter.limit_param_.offset_ = 0;
      const int64_t configured_timeout = 10000;
      const int64_t original_start_ts = INT64_MAX / 2;
      scan.set_pre_filtering_timeout(configured_timeout);
      scan.vec_start_scan_ts_us_ = original_start_ts;
      if (expected_ret == OB_SUCCESS && strategy == ObVecIdxQueryStrategy::LATENCY_FIRST) {
        ASSERT_FALSE(pre_filter_bitmap.is_contains(retained_entry.vid_));
        for (const bool post_fallback : {false, true}) {
          scan.pos_ = 0;
          iter.saved_rowkeys_.reuse();
          iter.adaptive_ctx_.is_brute_force_ = false;
          EXPECT_EQ(OB_SUCCESS, iter.run_async_pre_filter(
              allocator_, is_vectorized, post_fallback ? &retained : nullptr));
          EXPECT_EQ(configured_timeout, scan.vec_pre_filtering_timeout_us_);
          EXPECT_EQ(original_start_ts, scan.vec_start_scan_ts_us_);
          EXPECT_EQ(expected_count, iter.adaptive_ctx_.pre_scan_row_cnt_);
          EXPECT_EQ(expected_count > 0 && expected_count <= ObVecIdxExtraInfo::MAX_IVF_BRUTE_FORCE_SIZE,
                    iter.adaptive_ctx_.is_brute_force_);
          ASSERT_EQ(post_fallback ? 1 : 0, iter.saved_rowkeys_.count());
          if (post_fallback) {
            EXPECT_EQ(static_cast<uint64_t>(retained_entry.vid_),
                      iter.saved_rowkeys_.at(0).get_obj_ptr()[0].get_uint64());
          }
        }
      } else if (expected_ret != OB_SUCCESS) {
        // Real scan failures must still propagate; retained candidates cannot mask them.
        scan.pos_ = 0;
        EXPECT_EQ(expected_ret, iter.run_async_pre_filter(allocator_, is_vectorized, &retained));
        EXPECT_EQ(configured_timeout, scan.vec_pre_filtering_timeout_us_);
        EXPECT_EQ(original_start_ts, scan.vec_start_scan_ts_us_);
        EXPECT_EQ(0, iter.saved_rowkeys_.count());
      }
      if (verify_entry_budget) {
        ASSERT_EQ(ObVecIdxQueryStrategy::LATENCY_FIRST, strategy);
        ASSERT_EQ(OB_SUCCESS, expected_ret);
        class StatusWorker final : public lib::Worker
        {
        public:
          StatusWorker() : status_(OB_SUCCESS) {}
          int check_status() override { return status_; }
          int status_;
        } worker;
        lib::Worker *old_worker = &THIS_WORKER;
        lib::Worker::set_worker_to_thread_local(&worker);
        DEFER(lib::Worker::set_worker_to_thread_local(old_worker));
        ObIvfKnnSearchResult empty_retained;
        for (const bool has_post_result : {false, true}) {
          scan.pos_ = 0;
          scan.vec_start_scan_ts_us_ = 1;
          iter.saved_rowkeys_.reuse();
          iter.adaptive_ctx_.pre_scan_row_cnt_ = 0;
          iter.adaptive_ctx_.is_brute_force_ = false;
          const int64_t read_calls = scan.read_calls_;
          EXPECT_EQ(OB_SUCCESS, iter.run_async_pre_filter(
              allocator_, is_vectorized, has_post_result ? &retained : &empty_retained));
          EXPECT_EQ(read_calls, scan.read_calls_);
          EXPECT_EQ(0, scan.pos_);
          EXPECT_EQ(configured_timeout, scan.vec_pre_filtering_timeout_us_);
          EXPECT_EQ(1, scan.vec_start_scan_ts_us_);
          EXPECT_EQ(0, iter.adaptive_ctx_.pre_scan_row_cnt_);
          EXPECT_FALSE(iter.adaptive_ctx_.is_brute_force_);
          ASSERT_EQ(has_post_result ? 1 : 0, iter.saved_rowkeys_.count());
          if (has_post_result) {
            EXPECT_EQ(static_cast<uint64_t>(retained_entry.vid_),
                      iter.saved_rowkeys_.at(0).get_obj_ptr()[0].get_uint64());
          }
        }
        for (const int query_error : {OB_TIMEOUT, OB_CANCELED}) {
          worker.status_ = query_error;
          iter.saved_rowkeys_.reuse();
          const int64_t read_calls = scan.read_calls_;
          EXPECT_EQ(query_error, iter.run_async_pre_filter(allocator_, is_vectorized, &retained));
          EXPECT_EQ(read_calls, scan.read_calls_);
          EXPECT_EQ(0, iter.saved_rowkeys_.count());
        }
        worker.status_ = OB_SUCCESS;
        scan.set_pre_filtering_timeout(-1);
        scan.pos_ = 0;
        iter.saved_rowkeys_.reuse();
        const int64_t read_calls = scan.read_calls_;
        EXPECT_EQ(OB_SUCCESS, iter.run_async_pre_filter(allocator_, is_vectorized, &retained));
        EXPECT_GT(scan.read_calls_, read_calls);
        EXPECT_EQ(expected_count, iter.adaptive_ctx_.pre_scan_row_cnt_);
        EXPECT_EQ(-1, scan.vec_pre_filtering_timeout_us_);
        EXPECT_EQ(1, scan.vec_start_scan_ts_us_);
        ASSERT_EQ(1, iter.saved_rowkeys_.count());
      }
      ASSERT_EQ(1, retained.count());
      EXPECT_EQ(retained_entry.vid_, retained.at(0).vid_);
      EXPECT_EQ(retained_entry.distance_, retained.at(0).distance_);
    }
    iter.reset_async_read_state();
    EXPECT_EQ(0, iter.adaptive_ctx_.pre_scan_row_cnt_);
    EXPECT_FALSE(iter.deletion_bitmap_handle_.is_valid());
    iter.inv_idx_scan_iter_ = nullptr;
  }

  void verify_post_filter_profile(const bool is_vectorized)
  {
    const int64_t batch_size = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
    const int64_t row_count = batch_size + 7;
    ObExecContext exec_ctx(allocator_);
    ObEvalCtx eval_ctx(exec_ctx);
    ObDASVecAuxScanRtDef vec_rtdef;
    ObOpProfile<ObMetric> profile(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
    ObDASScanCtDef ctdef(allocator_);
    ObDASScanRtDef rtdef;
    ObDASIVFAsyncIter iter;
    MockAsyncIVFPostFilterScanIter scan;
    blocksstable::ObIvfKnnSearchResult round_result;
    ObIVFRowkeyDistMap candidates;
    ObSEArray<ObIvfRowkeyDistEntry, 16> matched;
    bool exists = false;
    uint64_t value = 0;
    ASSERT_EQ(OB_SUCCESS, candidates.create(32, ObMemAttr(MTL_ID(), "AsyncIVFTest")));
    for (int64_t i = 0; i < row_count; ++i) {
      blocksstable::ObIvfKnnEntry entry;
      entry.cid_ = 0;
      entry.vid_ = i + 1;
      entry.distance_ = static_cast<float>(i);
      ASSERT_EQ(OB_SUCCESS, round_result.add(entry));
    }
    ctdef.ref_table_id_ = 50001;
    iter.ls_id_ = ObLSID(1);
    iter.data_filter_tablet_id_ = ObTabletID(1001);
    iter.data_filter_ctdef_ = &ctdef;
    iter.data_filter_rtdef_ = &rtdef;
    iter.data_filter_iter_ = &scan;
    iter.data_filter_iter_first_scan_ = false;
    vec_rtdef.eval_ctx_ = &eval_ctx;
    iter.vec_aux_rtdef_ = &vec_rtdef;
    scan.set_scan_param(iter.data_filter_scan_param_);

    // Reject all candidates so each call completes both a full batch and a tail batch.
    for (int64_t round = 1; round <= 2; ++round) {
      candidates.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.add_async_ivf_filter_candidates(round_result, candidates));
      ASSERT_EQ(OB_SUCCESS, iter.do_table_post_filter(is_vectorized, candidates, matched, &profile));
      profile.get_metric_value(ObMetricId::VEC_ITER_FILTER_ROW, exists, value);
      ASSERT_TRUE(exists);
      EXPECT_EQ(round * row_count, value);
      profile.get_metric_value(ObMetricId::VEC_ITER_FILTER_LOOP_CNT, exists, value);
      ASSERT_TRUE(exists);
      EXPECT_EQ(round, value);
      EXPECT_EQ(round * 2, scan.rescan_count_);
      EXPECT_EQ(0, matched.count());
    }

    // A failed scan keeps the first submitted batch without resetting previous rounds.
    scan.rescan_ret_ = OB_TIMEOUT;
    EXPECT_EQ(OB_TIMEOUT, iter.do_table_post_filter(is_vectorized, candidates, matched, &profile));
    profile.get_metric_value(ObMetricId::VEC_ITER_FILTER_ROW, exists, value);
    ASSERT_TRUE(exists);
    EXPECT_EQ(2 * row_count + batch_size, value);
    profile.get_metric_value(ObMetricId::VEC_ITER_FILTER_LOOP_CNT, exists, value);
    ASSERT_TRUE(exists);
    EXPECT_EQ(3, value);
    EXPECT_EQ(5, scan.rescan_count_);
    // Error cleanup may still borrow the submitted keys after filtering returns.
    ASSERT_GT(iter.data_filter_scan_param_.key_ranges_.count(), 0);
    for (const auto &range : iter.data_filter_scan_param_.key_ranges_) {
      const ObRowkey &key = range.get_start_key();
      ASSERT_NE(nullptr, candidates.get(key));
      EXPECT_GE(key.get_obj_ptr()[0].get_uint64(), 1);
      EXPECT_LE(key.get_obj_ptr()[0].get_uint64(), static_cast<uint64_t>(row_count));
    }

    candidates.reuse();
    EXPECT_EQ(OB_SUCCESS, iter.do_table_post_filter(is_vectorized, candidates, matched, &profile));
    profile.get_metric_value(ObMetricId::VEC_ITER_FILTER_ROW, exists, value);
    ASSERT_TRUE(exists);
    EXPECT_EQ(2 * row_count + batch_size, value);
    profile.get_metric_value(ObMetricId::VEC_ITER_FILTER_LOOP_CNT, exists, value);
    ASSERT_TRUE(exists);
    EXPECT_EQ(3, value);
    iter.data_filter_iter_ = nullptr;
  }

  ObTenantBase tenant_base_{OB_SYS_TENANT_ID};
  ObArenaAllocator allocator_{"AsyncIVFIter"};
};

// Enforce the storage scan contract without an observer: a rescan needs an
// opened tree, and reuse preserves that tree rather than permitting another open.
class MockAsyncIVFPreFilterIter : public ObDASSearchDriverIter
{
public:
  int do_table_scan() override
  {
    ++open_count_;
    int ret = opened_ ? OB_INIT_TWICE : OB_SUCCESS;
    opened_ = true;
    return ret;
  }

  int rescan() override
  {
    ++rescan_count_;
    return opened_ ? OB_SUCCESS : OB_NOT_INIT;
  }

  int64_t open_count_ = 0;
  int64_t rescan_count_ = 0;
  bool opened_ = false;
};

TEST_F(TestDASAsyncIVFScanIter, hybrid_pre_filter_partition_lifecycle)
{
  // Exercise both a first partition that skips the ordinary PRE tree and one
  // that opens it immediately. The catalog is fixed, so no compaction race can
  // make either transition disappear from the regression.
  const int64_t group_counts[][5] = {{2, 1, 2, 1, 1}, {1, 2, 1, 2, 1}};
  for (const auto &counts : group_counts) {
    SCOPED_TRACE(::testing::Message() << "first partition groups=" << counts[0]);
    ObDASScanOp scan_op(allocator_);
    ObDASSearchCtx search_ctx(allocator_, scan_op);
    ObDASAsyncIndexQueryCtx query_ctx;
    query_ctx.group_alignment_enabled_ = true;
    search_ctx.async_query_ctx_ = &query_ctx;
    ObAsyncIndexPartitionSegCtx partitions[2];
    for (int64_t p = 0; p < 2; ++p) {
      auto &catalog = partitions[p].storage_group_ctx_;
      for (int64_t g = 0; g <= p; ++g) {
        auto *group = OB_NEWx(storage::ObAsyncIndexSegmentGroup, &catalog.allocator_);
        ASSERT_NE(nullptr, group);
        ASSERT_EQ(OB_SUCCESS, group->init(1));
        ObNewVersionRange range;
        range.base_version_ = g * 100;
        range.snapshot_version_ = (g + 1) * 100;
        ASSERT_EQ(OB_SUCCESS, group->add_version_range(0, range));
        ASSERT_EQ(OB_SUCCESS, catalog.groups_.push_back(group));
      }
    }

    ObDASScalarScanCtDef filter_ctdef(allocator_);
    ObDASScalarScanRtDef filter_rtdef;
    filter_ctdef.is_async_index_ = true;
    filter_ctdef.ref_table_id_ = 60001;
    filter_rtdef.ctdef_ = &filter_ctdef;
    ObDASVecIndexDriverCtDef driver_ctdef(allocator_);
    driver_ctdef.filter_mode_ = ObKnnFilterMode::PRE_KNN;
    driver_ctdef.vec_type_ = ObVecIndexType::VEC_INDEX_PRE;
    ObDASVecIndexAsyncIVFScanIter scan;
    scan.inited_ = true;
    scan.search_ctx_ = &search_ctx;
    scan.filter_rtdef_ = &filter_rtdef;
    MockAsyncIVFPreFilterIter filter;
    filter.inited_ = true;
    ObDASVecIndexDriverIter driver;
    driver.inited_ = true;
    driver.init_complete_ = true;
    driver.vec_index_scan_iter_ = &scan;
    driver.pre_filter_iter_ = &filter;
    driver.filter_iter_ = &filter;
    driver.search_ctx_ = &search_ctx;
    driver.vec_index_driver_ctdef_ = &driver_ctdef;
    DEFER(
      EXPECT_EQ(OB_SUCCESS, driver.release());
      EXPECT_FALSE(scan.is_inited());
      EXPECT_FALSE(filter.is_inited());
    );

    int64_t ordinary_partition_count = 0;
    for (int64_t p = 0; p < ARRAYSIZEOF(counts); ++p) {
      SCOPED_TRACE(::testing::Message() << "partition=" << p << ", groups=" << counts[p]);
      search_ctx.set_current_async_partition(&partitions[counts[p] - 1]);
      if (0 == p) {
        ASSERT_EQ(OB_SUCCESS, driver.evaluate_partition_path());
        ASSERT_EQ(OB_SUCCESS, driver.do_table_scan());
      } else {
        ASSERT_EQ(OB_SUCCESS, driver.reuse());
        ASSERT_EQ(OB_SUCCESS, driver.rescan());
      }
      ASSERT_EQ(counts[p] > 1, driver.uses_group_pre_filter());
      ordinary_partition_count += 1 == counts[p] ? 1 : 0;
      EXPECT_EQ(ordinary_partition_count > 0 ? 1 : 0, filter.open_count_);
      EXPECT_EQ(ordinary_partition_count > 0 ? ordinary_partition_count - 1 : 0,
                filter.rescan_count_);
    }
  }
}

TEST_F(TestDASAsyncIVFScanIter, ordinary_query_builds_async_ivf_local_lookup_tree)
{
  verify_runtime_tree(ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER);
}

TEST_F(TestDASAsyncIVFScanIter, iterative_filter_builds_async_ivf_local_lookup_tree)
{
  verify_runtime_tree(ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER);
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_reuses_primary_scan_param)
{
  verify_runtime_tree(ObVecIndexType::VEC_INDEX_PRE, false, true);
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_reuses_scalar_index_scan_param)
{
  verify_runtime_tree(ObVecIndexType::VEC_INDEX_PRE);
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_builds_local_lookup_for_residual_predicates)
{
  verify_runtime_tree(ObVecIndexType::VEC_INDEX_PRE, true);
}

TEST_F(TestDASAsyncIVFScanIter, adaptive_pre_filter_rejects_missing_estimator)
{
  verify_runtime_tree(ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN, true, false,
                     ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER, OB_ERR_UNEXPECTED);
}

TEST_F(TestDASAsyncIVFScanIter, adaptive_post_filter_rejects_missing_estimator)
{
  verify_runtime_tree(ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN, true, false,
                     ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER, OB_ERR_UNEXPECTED);
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_empty_input)
{
  verify_pre_filter_materialization(true, nullptr, 0, -1, OB_SUCCESS, 0);
}

TEST_F(TestDASAsyncIVFScanIter, partition_filter_estimate_boundaries)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  ObDASIVFAsyncIter iter;
  ctdef.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
  ctdef.selectivity_ = 0.75;
  ctdef.row_count_ = 200000;
  iter.vec_aux_ctdef_ = &ctdef;
  iter.vec_index_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  // Test the 10% boundary above the small-result pre-filter row-count limit.
  const int64_t partition_rows[] = {20000, 20000, 1000000, 1000000, 1000000,
      1000000, 1000000, 100000, 100000, 100000, 0, -1, 100000};
  const int64_t filter_rows[] = {10000, 10001, 99999, 100000, 100001,
      200000, 300000, 95000, 0, 200000, 0, 0, -1};
  const bool expect_pre[] = {true, false, true, true, false,
      false, false, false, true, false, false, false, false};
  for (int64_t i = 0; i < ARRAYSIZEOF(partition_rows); ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, partition_rows[i], filter_rows[i]));
    EXPECT_EQ(expect_pre[i], iter.is_pre_filter()) << i;
    EXPECT_EQ(ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN, iter.vec_index_type_);
    EXPECT_GE(iter.selectivity_, 0.0);
    EXPECT_LE(iter.selectivity_, 1.0);
    EXPECT_EQ(iter.selectivity_, iter.adaptive_ctx_.selectivity_);
    EXPECT_EQ(partition_rows[i] > 0 && filter_rows[i] >= 0 ? partition_rows[i] : ctdef.row_count_,
              iter.adaptive_ctx_.row_count_);
    EXPECT_FALSE(iter.partition_scan_finished_); // F=0 is an estimate, not an empty result.
  }
}

TEST_F(TestDASAsyncIVFScanIter, explicit_paths_skip_partition_estimation)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  ObDASIVFAsyncIter iter;
  iter.vec_aux_ctdef_ = &ctdef;
  const ObVecIndexType types[] = {ObVecIndexType::VEC_INDEX_PRE,
      ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER, ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER};
  for (int64_t i = 0; i < ARRAYSIZEOF(types); ++i) {
    iter.vec_index_type_ = types[i];
    // No input or storage service: fixed paths must not even enter estimation.
    EXPECT_EQ(OB_SUCCESS, iter.refresh_partition_filter_path());
    EXPECT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 100000, 1));
    EXPECT_EQ(types[i], iter.vec_index_type_);
    EXPECT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 100000, 99000));
    EXPECT_EQ(types[i], iter.vec_index_type_);
  }
}

TEST_F(TestDASAsyncIVFScanIter, hinted_adaptive_paths_skip_estimation_across_reuse)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  ObDASIVFAsyncIter iter;
  ctdef.vec_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  iter.vec_aux_ctdef_ = &ctdef;
  iter.vec_index_type_ = ctdef.vec_type_;
  iter.is_adaptive_specify_by_hint_ = true;
  const ObVecIdxAdaTryPath paths[] = {ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER,
      ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER};
  for (int64_t i = 0; i < ARRAYSIZEOF(paths); ++i) {
    ctdef.adaptive_try_path_ = paths[i];
    iter.vec_idx_try_path_ = paths[i];
    ASSERT_TRUE(iter.is_adaptive_filter());
    // No scalar input or storage service is bound: hinted paths must bypass estimation.
    ASSERT_EQ(OB_SUCCESS, iter.refresh_partition_filter_path());
    EXPECT_EQ(paths[i], iter.vec_idx_try_path_);
    ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
    EXPECT_TRUE(iter.is_adaptive_specify_by_hint_);
    ASSERT_EQ(OB_SUCCESS, iter.refresh_partition_filter_path());
    EXPECT_EQ(paths[i], iter.vec_idx_try_path_);
  }
  // An automatically selected PRE/POST must still enter estimation on this same tree.
  iter.is_adaptive_specify_by_hint_ = false;
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.refresh_partition_filter_path());
}

TEST_F(TestDASAsyncIVFScanIter, partition_estimates_are_private_and_failure_restores_plan)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  ObDASVecAuxScanRtDef rtdef;
  ObDASIVFAsyncIter first;
  ObDASIVFAsyncIter second;
  ctdef.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
  ctdef.selectivity_ = 0.8;
  ctdef.row_count_ = 1000000;
  first.vec_aux_ctdef_ = second.vec_aux_ctdef_ = &ctdef;
  first.vec_aux_rtdef_ = second.vec_aux_rtdef_ = &rtdef;
  first.vec_index_type_ = second.vec_index_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  ASSERT_EQ(OB_SUCCESS, first.apply_partition_filter_estimate(OB_SUCCESS, 200000, 20000));
  ASSERT_EQ(OB_SUCCESS, second.apply_partition_filter_estimate(OB_SUCCESS, 200000, 150000));
  EXPECT_TRUE(first.is_pre_filter());
  EXPECT_TRUE(second.is_iter_filter());
  EXPECT_DOUBLE_EQ(0.1, first.selectivity_);
  EXPECT_DOUBLE_EQ(0.75, second.selectivity_);
  EXPECT_DOUBLE_EQ(0.8, ctdef.selectivity_);
  EXPECT_EQ(1000000, ctdef.row_count_);
  EXPECT_EQ(ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER, ctdef.adaptive_try_path_);
  ASSERT_EQ(OB_SUCCESS, first.apply_partition_filter_estimate(OB_TABLET_NOT_EXIST, -1, -1));
  EXPECT_TRUE(first.is_iter_filter());
  EXPECT_DOUBLE_EQ(ctdef.selectivity_, first.selectivity_);
  EXPECT_EQ(ctdef.row_count_, first.adaptive_ctx_.row_count_);
  EXPECT_DOUBLE_EQ(0.75, second.selectivity_);
  // Same partition, new ranges: no cached estimate can keep the previous path.
  ASSERT_EQ(OB_SUCCESS, first.apply_partition_filter_estimate(OB_SUCCESS, 100000, 95000));
  EXPECT_TRUE(first.is_iter_filter());
  ASSERT_EQ(OB_SUCCESS, first.apply_partition_filter_estimate(OB_SUCCESS, 100000, 0));
  EXPECT_TRUE(first.is_pre_filter());
}

TEST_F(TestDASAsyncIVFScanIter, partition_estimation_rejects_invalid_context)
{
  ObDASScanCtDef ctdef(allocator_);
  ObDASIVFAsyncIter iter;
  ObQueryFlag scan_flag;
  ObSEArray<ObNewRange, 1> ranges;
  int64_t row_count = 100;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            iter.estimate_partition_rows(ctdef, ObTabletID(100001), scan_flag, ranges, row_count));
  EXPECT_EQ(-1, row_count);
  iter.ls_id_ = ObLSID(1);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            iter.estimate_partition_rows(ctdef, ObTabletID(), scan_flag, ranges, row_count));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            iter.estimate_partition_rows(ctdef, ObTabletID(100001), scan_flag, ranges, row_count));
  EXPECT_EQ(-1, row_count);
}

TEST_F(TestDASAsyncIVFScanIter, partition_estimation_propagates_state_and_query_failures)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  ObDASIVFAsyncIter iter;
  ctdef.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
  ctdef.selectivity_ = 0.8;
  ctdef.row_count_ = 100000;
  iter.vec_aux_ctdef_ = &ctdef;
  iter.vec_index_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  const int errors[] = {OB_INVALID_ARGUMENT, OB_ERR_UNEXPECTED, OB_NOT_INIT,
      OB_TIMEOUT, OB_ALLOCATE_MEMORY_FAILED, OB_TENANT_OUT_OF_MEM,
      OB_CANCELED, OB_ERR_QUERY_INTERRUPTED, OB_ERR_SESSION_INTERRUPTED, OB_SESSION_KILLED, OB_ERR_INTERRUPTED};
  for (int64_t i = 0; i < ARRAYSIZEOF(errors); ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 100000, 100));
    ASSERT_TRUE(iter.is_pre_filter());
    EXPECT_EQ(errors[i], iter.apply_partition_filter_estimate(errors[i], -1, -1));
    EXPECT_TRUE(iter.is_iter_filter());
    EXPECT_DOUBLE_EQ(ctdef.selectivity_, iter.selectivity_);
    EXPECT_EQ(ctdef.row_count_, iter.adaptive_ctx_.row_count_);
  }
}

TEST_F(TestDASAsyncIVFScanIter, partition_paths_share_input_lifecycle_and_lazy_lookup_context)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  ObDASIVFAsyncIter iter;
  ObDASLocalLookupIter lookup;
  MockAsyncIVFLookupScanIter input;
  MockAsyncIVFLookupScanIter data;
  ObTableScanParam scan_param;
  transaction::ObTxReadSnapshot snapshot;
  input.set_scan_param(scan_param);
  lookup.index_table_iter_ = &input;
  lookup.data_table_iter_ = &data;
  iter.vec_aux_ctdef_ = &ctdef;
  iter.snapshot_ = &snapshot;
  iter.vec_index_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  ASSERT_EQ(OB_SUCCESS, iter.init_filter_input(&lookup));
  ASSERT_EQ(OB_SUCCESS, snapshot.core_.version_.convert_for_tx(1000));
  // Even POST opens the input. Unopened scans initialize despite stale switch flags.
  scan_param.need_switch_param_ = true;
  lookup.get_lookup_param().need_switch_param_ = true;
  ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 100000, 90000));
  ASSERT_EQ(OB_SUCCESS, iter.start_filter_input(false));
  EXPECT_EQ(1, input.scan_count_);
  EXPECT_EQ(0, input.rescan_count_);
  EXPECT_EQ(1000, input.observed_snapshot_);
  EXPECT_FALSE(scan_param.need_switch_param_);
  EXPECT_FALSE(lookup.get_lookup_param().need_switch_param_);
  EXPECT_EQ(0, data.scan_count_);
  // The mock represents an opened input storage iterator from here on.
  input.get_output_result_iter() = &input;
  ASSERT_EQ(OB_SUCCESS, snapshot.core_.version_.convert_for_tx(2000));
  scan_param.need_switch_param_ = true;
  lookup.get_lookup_param().need_switch_param_ = true;
  ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 100000, 100));
  ASSERT_EQ(OB_SUCCESS, iter.start_filter_input(true));
  EXPECT_EQ(1, input.scan_count_);
  EXPECT_EQ(1, input.rescan_count_);
  EXPECT_EQ(2000, input.observed_snapshot_);
  EXPECT_TRUE(scan_param.need_switch_param_);
  EXPECT_FALSE(lookup.get_lookup_param().need_switch_param_);
  EXPECT_EQ(2000, lookup.get_lookup_param().snapshot_.version().get_val_for_tx());
  ASSERT_EQ(OB_SUCCESS, snapshot.core_.version_.convert_for_tx(3000));
  scan_param.need_switch_param_ = true;
  lookup.get_lookup_param().need_switch_param_ = true;
  ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 100000, 90000));
  ASSERT_EQ(OB_SUCCESS, iter.start_filter_input(true));
  EXPECT_EQ(2, input.rescan_count_);
  EXPECT_EQ(3000, input.observed_snapshot_);
  EXPECT_EQ(3000, lookup.get_lookup_param().snapshot_.version().get_val_for_tx());
  EXPECT_FALSE(lookup.get_lookup_param().need_switch_param_);
  ASSERT_EQ(OB_SUCCESS, snapshot.core_.version_.convert_for_tx(4000));
  ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 100000, 100));
  ASSERT_EQ(OB_SUCCESS, iter.start_filter_input(true));
  EXPECT_EQ(1, input.scan_count_);
  EXPECT_EQ(3, input.rescan_count_);
  EXPECT_EQ(4000, input.observed_snapshot_);
  EXPECT_TRUE(scan_param.need_switch_param_);
  // An earlier empty input can leave the lookup unopened even when input scan was opened.
  EXPECT_FALSE(lookup.get_lookup_param().need_switch_param_);
  EXPECT_EQ(0, data.scan_count_);
  EXPECT_EQ(0, data.rescan_count_);
  data.get_output_result_iter() = &data;
  lookup.get_lookup_param().need_switch_param_ = true;
  input.rescan_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, iter.start_filter_input(true));
  EXPECT_TRUE(lookup.get_lookup_param().need_switch_param_);
  input.get_output_result_iter() = nullptr;
  data.get_output_result_iter() = nullptr;
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_scalar_deduplicates_without_rechecking_deletion_bitmap)
{
  const uint64_t rowids[] = {1, 2, 2, 3, 4};
  verify_pre_filter_materialization(false, rowids, ARRAYSIZEOF(rowids), -1, OB_SUCCESS, 4);
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_consumes_last_valid_batch_with_iter_end)
{
  const uint64_t rowids[] = {1, 2, 2, 3, 4};
  verify_pre_filter_materialization(true, rowids, ARRAYSIZEOF(rowids), -1, OB_SUCCESS, 4);
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_discards_partial_bitmap_on_scan_error)
{
  const uint64_t rowids[] = {1, 2, 2, 3, 4};
  verify_pre_filter_materialization(true, rowids, ARRAYSIZEOF(rowids), 2, OB_TIMEOUT, 0);
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_rejects_invalid_vid_and_discards_partial_bitmap)
{
  const uint64_t rowids[] = {1, 2, UINT64_MAX};
  verify_pre_filter_materialization(true, rowids, ARRAYSIZEOF(rowids), -1, OB_ERR_UNEXPECTED, 0);
}

TEST_F(TestDASAsyncIVFScanIter, latency_pre_filter_preserves_only_successful_rows)
{
  const uint64_t rowids[] = {1, 2, 3, 4, 5};
  for (int mode = 0; mode < 2; ++mode) {
    verify_pre_filter_materialization(mode != 0, rowids, ARRAYSIZEOF(rowids), 0,
        OB_SUCCESS, 0, OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY, ObVecIdxQueryStrategy::LATENCY_FIRST);
    // Scalar mode reaches the timeout inside a partially collected batch.
    verify_pre_filter_materialization(mode != 0, rowids, ARRAYSIZEOF(rowids), 2,
        OB_SUCCESS, 2, OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY, ObVecIdxQueryStrategy::LATENCY_FIRST, 4);
    verify_pre_filter_materialization(mode != 0, rowids, ARRAYSIZEOF(rowids), 2,
        OB_TIMEOUT, 0, OB_TIMEOUT, ObVecIdxQueryStrategy::LATENCY_FIRST, 4);
    verify_pre_filter_materialization(mode != 0, rowids, ARRAYSIZEOF(rowids), 2,
        OB_IO_ERROR, 0, OB_IO_ERROR, ObVecIdxQueryStrategy::LATENCY_FIRST, 4);
    verify_pre_filter_materialization(mode != 0, rowids, ARRAYSIZEOF(rowids), 2,
        OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY, 0, OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY);
  }
}

TEST_F(TestDASAsyncIVFScanIter, latency_post_fallback_checks_budget_before_reading_input)
{
  const uint64_t rowids[] = {1, 2, 3, 4, 5};
  for (const bool is_vectorized : {false, true}) {
    verify_pre_filter_materialization(is_vectorized, rowids, ARRAYSIZEOF(rowids), 2,
        OB_SUCCESS, 2, OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY,
        ObVecIdxQueryStrategy::LATENCY_FIRST, 4, true);
  }
}

TEST_F(TestDASAsyncIVFScanIter, latency_pre_filter_preserves_bitmap_above_brute_force_threshold)
{
  ObSEArray<uint64_t, 16> rowids;
  const int64_t count = ObVecIdxExtraInfo::MAX_IVF_BRUTE_FORCE_SIZE + 2;
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, rowids.push_back(i));
  }
  for (int mode = 0; mode < 2; ++mode) {
    verify_pre_filter_materialization(mode != 0, rowids.get_data(), count, count - 1,
        OB_SUCCESS, count - 1, OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY,
        ObVecIdxQueryStrategy::LATENCY_FIRST, 64);
  }
}

TEST_F(TestDASAsyncIVFScanIter, compact_pre_filter_materializes_rowkey_batches_and_tail)
{
  const int64_t row_count = 1031;
  const int64_t distinct_count = 521;
  uint64_t rowids[row_count] = {0};
  for (int64_t i = 0; i < row_count; ++i) {
    rowids[i] = 1024 + distinct_count - 1 - i % distinct_count;
  }
  for (int64_t mode = 0; mode < 2; ++mode) {
    verify_pre_filter_materialization(mode != 0, rowids, row_count, -1,
        OB_SUCCESS, distinct_count, OB_TIMEOUT, ObVecIdxQueryStrategy::RECALL_FIRST, 127);
    verify_pre_filter_materialization(mode != 0, rowids, row_count, 259,
        OB_TIMEOUT, 0, OB_TIMEOUT, ObVecIdxQueryStrategy::RECALL_FIRST, 127);
  }
}

TEST_F(TestDASAsyncIVFScanIter, compact_pre_filter_preserves_exact_brute_force_boundary)
{
  blocksstable::ObIvfPreFilterBitmap bitmap(allocator_);
  ObDASIVFAsyncIter iter;
  // Fail preparation after the dispatch decision, without opening storage.
  iter.limit_param_.limit_ = 0;
  iter.limit_param_.offset_ = 0;
  for (uint64_t i = 0; i < 10000; ++i) {
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(i));
  }
  ASSERT_TRUE(bitmap.is_compact());
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(9999));
  ASSERT_EQ(10000, bitmap.get_cardinality());
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.scan_pre_filtered_segments(bitmap));
  ASSERT_TRUE(iter.adaptive_ctx_.is_brute_force_);
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(10000));
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.scan_pre_filtered_segments(bitmap));
  ASSERT_FALSE(iter.adaptive_ctx_.is_brute_force_);
  iter.reset_async_read_state();
  bitmap.reset();
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(7));
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.scan_pre_filtered_segments(bitmap));
  ASSERT_TRUE(iter.adaptive_ctx_.is_brute_force_);
}

TEST_F(TestDASAsyncIVFScanIter, latency_strategy_arms_async_filter_input)
{
  verify_runtime_tree(ObVecIndexType::VEC_INDEX_PRE, false, true,
      ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER, OB_SUCCESS, ObVecIdxQueryStrategy::LATENCY_FIRST);
  verify_runtime_tree(ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER, false, true,
      ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER, OB_SUCCESS, ObVecIdxQueryStrategy::LATENCY_FIRST);
}

TEST_F(TestDASAsyncIVFScanIter, residual_lookup_creation_failure_releases_only_owned_nodes)
{
  float query_vector[2] = {1.0F, 0.0F};
  ObExecContext exec_ctx(allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  ObExpr rowid_expr;
  ObExpr query_expr;
  ObExpr limit_expr;
  ObExpr distance_expr;
  ASSERT_EQ(OB_SUCCESS, init_vector_lookup_exprs(eval_ctx, query_vector, sizeof(query_vector),
      rowid_expr, query_expr, limit_expr, distance_expr));
  ObDASScanCtDef index_ctdef(allocator_);
  ObDASScanRtDef index_rtdef;
  ObDASScanCtDef lookup_ctdef(allocator_);
  ObDASScanRtDef lookup_rtdef;
  ASSERT_EQ(OB_SUCCESS, lookup_ctdef.rowkey_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, lookup_ctdef.rowkey_exprs_.push_back(&rowid_expr));
  lookup_rtdef.eval_ctx_ = &eval_ctx;
  lookup_rtdef.ctdef_ = &lookup_ctdef;
  index_rtdef.eval_ctx_ = &eval_ctx;
  index_rtdef.ctdef_ = &index_ctdef;
  ObTableScanParam scan_param;
  scan_param.ls_id_ = ObLSID(1);
  transaction::ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.core_.version_.convert_for_tx(120));
  MockAsyncIVFPreFilterScanIter index_iter(eval_ctx, rowid_expr, nullptr, 0, -1);
  // Fail the data scan object, LocalLookup object, and children array in turn.
  for (int64_t fail_at = 1; fail_at <= 3; ++fail_at) {
    AsyncIVFFailAllocator fail_allocator(allocator_, fail_at);
    ObDASLocalLookupIter *lookup_iter = nullptr;
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObDASIterUtils::create_local_lookup_sub_tree(
        scan_param, fail_allocator, &index_ctdef, &index_rtdef, &lookup_ctdef,
        &lookup_rtdef, nullptr, &snapshot, &index_iter, ObTabletID(100001), lookup_iter));
    EXPECT_EQ(nullptr, lookup_iter);
    // Object storage lives in the query arena; guards must release its resources.
    ASSERT_EQ(static_cast<size_t>(fail_at - 1), fail_allocator.allocations_.size());
    for (void *ptr : fail_allocator.allocations_) {
      auto *iter = static_cast<ObDASIter *>(ptr);
      EXPECT_FALSE(iter->is_inited());
      EXPECT_EQ(DAS_ITER_INVALID, iter->get_type());
    }
    if (fail_at == 3) {
      auto *released_lookup = static_cast<ObDASLocalLookupIter *>(
          static_cast<ObDASIter *>(fail_allocator.allocations_.back()));
      EXPECT_EQ(nullptr, released_lookup->lookup_memctx_);
    }
    EXPECT_TRUE(index_iter.inited_);
  }
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_merges_segment_topk_before_limit_and_offset)
{
  const int64_t vids[] = {60, 20, 30, 10, 40, 50, 70};
  const float distances[] = {8.0F, 2.0F, 3.0F, 1.0F, 4.0F, 9.0F, 6.0F};
  ObSEArray<blocksstable::ObIvfKnnEntry, 16> candidates;
  for (int64_t i = 0; i < ARRAYSIZEOF(vids); ++i) {
    blocksstable::ObIvfKnnEntry entry;
    entry.cid_ = i < 3 ? 0 : 1;
    entry.vid_ = vids[i];
    entry.distance_ = distances[i];
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(entry));
  }
  ObDASIVFAsyncIter iter;
  iter.limit_param_.limit_ = 3;
  iter.limit_param_.offset_ = 1;
  ASSERT_EQ(OB_SUCCESS, iter.append_pre_filter_topk(candidates));
  ASSERT_EQ(4, iter.saved_rowkeys_.count());
  const uint64_t expected_vids[] = {10, 20, 30, 40};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_vids); ++i) {
    EXPECT_EQ(expected_vids[i], iter.saved_rowkeys_.at(i).get_obj_ptr()[0].get_uint64());
  }
}

TEST_F(TestDASAsyncIVFScanIter, post_filter_stagnation_resets_on_growth)
{
  ObDASIVFAsyncIter::PostFilterProgress progress;
  EXPECT_FALSE(progress.finish_window(2)); // First window makes progress.
  EXPECT_FALSE(progress.finish_window(2));
  EXPECT_FALSE(progress.finish_window(2));
  EXPECT_FALSE(progress.finish_window(3)); // New VID resets the two stalled windows.
  EXPECT_EQ(0, progress.stalled_windows_);
  EXPECT_FALSE(progress.finish_window(3));
  EXPECT_FALSE(progress.finish_window(3));
  EXPECT_TRUE(progress.finish_window(3));
  EXPECT_EQ(3, progress.stalled_windows_);
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_preserves_and_deduplicates_retained_post_candidates)
{
  blocksstable::ObIvfKnnSearchResult retained;
  ObSEArray<blocksstable::ObIvfKnnEntry, 16> candidates;
  for (int64_t vid : {20, 30}) {
    blocksstable::ObIvfKnnEntry entry;
    entry.vid_ = vid;
    entry.distance_ = vid / 10.0F;
    ASSERT_EQ(OB_SUCCESS, retained.add(entry));
  }
  for (int64_t vid : {10, 20, 40}) {
    blocksstable::ObIvfKnnEntry entry;
    entry.vid_ = vid;
    entry.distance_ = vid / 10.0F;
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(entry));
  }
  ObDASIVFAsyncIter iter;
  iter.limit_param_.limit_ = 3;
  iter.limit_param_.offset_ = 1;
  ASSERT_EQ(OB_SUCCESS, iter.append_pre_filter_topk(candidates, &retained));
  ASSERT_EQ(4, iter.saved_rowkeys_.count());
  const uint64_t expected[] = {10, 20, 30, 40};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected); ++i) {
    EXPECT_EQ(expected[i], iter.saved_rowkeys_.at(i).get_obj_ptr()[0].get_uint64());
  }
}

TEST_F(TestDASAsyncIVFScanIter, pre_filter_invalid_segment_candidate_emits_no_partial_topk)
{
  ObSEArray<blocksstable::ObIvfKnnEntry, 16> candidates;
  blocksstable::ObIvfKnnEntry entry;
  entry.vid_ = 10;
  entry.distance_ = 1.0F;
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(entry));
  entry.vid_ = -1;
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(entry));
  ObDASIVFAsyncIter iter;
  iter.limit_param_.limit_ = 3;
  EXPECT_EQ(OB_INVALID_DATA, iter.append_pre_filter_topk(candidates));
  EXPECT_EQ(0, iter.saved_rowkeys_.count());
}

TEST_F(TestDASAsyncIVFScanIter, post_filter_rowkeys_reuse_memory_across_rounds)
{
  ObDASIVFAsyncIter iter;
  ObIVFRowkeyDistMap candidates;
  ASSERT_EQ(OB_SUCCESS, candidates.create(32, ObMemAttr(MTL_ID(), "AsyncIVFTest")));
  const int64_t arena_used = iter.vec_op_alloc_.used();
  const ObObj *buffer = nullptr;
  // Include rounds larger than the former fixed candidate batch and a tail.
  for (int64_t round = 0; round < 20; ++round) {
    blocksstable::ObIvfKnnSearchResult result;
    const int64_t count = 0 == round % 2 ? 513 : 257;
    for (int64_t i = 0; i < count; ++i) {
      blocksstable::ObIvfKnnEntry entry;
      entry.vid_ = round * 1000 + i;
      entry.distance_ = static_cast<float>(i);
      ASSERT_EQ(OB_SUCCESS, result.add(entry));
    }
    candidates.reuse();
    ASSERT_EQ(OB_SUCCESS, iter.add_async_ivf_filter_candidates(result, candidates));
    EXPECT_EQ(count, candidates.size());
    EXPECT_EQ(arena_used, iter.vec_op_alloc_.used());
    if (nullptr == buffer) {
      buffer = iter.filter_rowkey_objs_.get_data();
    }
    EXPECT_EQ(buffer, iter.filter_rowkey_objs_.get_data());
    for (int64_t i = 0; i < count; ++i) {
      ObObj key;
      key.set_uint64(round * 1000 + i);
      const ObIvfRowkeyDistEntry *entry = candidates.get(ObRowkey(&key, 1));
      ASSERT_NE(nullptr, entry);
      EXPECT_FLOAT_EQ(static_cast<float>(i), entry->distance_);
      EXPECT_EQ(key.get_uint64(), entry->rowkey_.get_obj_ptr()[0].get_uint64());
    }
  }
  candidates.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
  EXPECT_EQ(0, iter.filter_rowkey_objs_.count());
  EXPECT_LT(iter.filter_rowkey_objs_.get_capacity(), 513);
}

TEST_F(TestDASAsyncIVFScanIter, post_filter_duplicate_batches_do_not_stop_refill)
{
  ObDASIVFAsyncIter iter;
  iter.limit_param_.limit_ = 100;
  iter.limit_param_.offset_ = 0;
  blocksstable::ObIvfTopKCollector collector;
  ASSERT_EQ(OB_SUCCESS, collector.init(100, allocator_));
  ObObj rowkeys[100];
  for (int64_t i = 0; i < 100; ++i) {
    rowkeys[i].set_uint64(i);
  }
  ObSEArray<ObIvfRowkeyDistEntry, 16> candidates;
  int64_t collected_count = 0;
  for (int64_t i = 0; i < 95; ++i) {
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(
        ObIvfRowkeyDistEntry(ObRowkey(&rowkeys[i], 1), static_cast<float>(i + 1))));
  }
  ASSERT_EQ(OB_SUCCESS, iter.collect_filtered_async_ivf_candidates(
      candidates, collector, collected_count));
  ASSERT_EQ(95, collected_count);

  // A later search round can return VIDs already retained in the heap.
  // Better distances update the heap but must not satisfy the refill count.
  candidates.reuse();
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(
        ObIvfRowkeyDistEntry(ObRowkey(&rowkeys[i], 1), static_cast<float>(i) + 0.5F)));
  }
  ASSERT_EQ(OB_SUCCESS, iter.collect_filtered_async_ivf_candidates(
      candidates, collector, collected_count));
  ASSERT_EQ(95, collected_count);
  ASSERT_LT(collected_count, iter.limit_param_.limit_);

  candidates.reuse();
  for (int64_t i = 95; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(
        ObIvfRowkeyDistEntry(ObRowkey(&rowkeys[i], 1), static_cast<float>(i + 1))));
  }
  ASSERT_EQ(OB_SUCCESS, iter.collect_filtered_async_ivf_candidates(
      candidates, collector, collected_count));
  EXPECT_EQ(100, collected_count);
  ASSERT_EQ(OB_SUCCESS, iter.append_async_ivf_topk(collector));
  ASSERT_EQ(100, iter.saved_rowkeys_.count());
  for (int64_t i = 0; i < 100; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i), iter.saved_rowkeys_.at(i).get_obj_ptr()[0].get_uint64());
  }
}

TEST_F(TestDASAsyncIVFScanIter, post_filter_unique_count_caps_at_limit_plus_offset)
{
  ObDASIVFAsyncIter iter;
  iter.limit_param_.limit_ = 2;
  iter.limit_param_.offset_ = 1;
  blocksstable::ObIvfTopKCollector collector;
  ASSERT_EQ(OB_SUCCESS, collector.init(3, allocator_));
  // Includes a duplicate within the first batch, a full-heap replacement,
  // an ignored worse candidate, and a better distance for a retained VID.
  const uint64_t vids[] = {10, 20, 20, 30, 40, 50, 20};
  const float distances[] = {3.0F, 2.0F, 4.0F, 1.0F, 0.5F, 10.0F, 0.1F};
  ObObj rowkeys[ARRAYSIZEOF(vids)];
  ObSEArray<ObIvfRowkeyDistEntry, 16> candidates;
  int64_t collected_count = 0;
  for (int64_t i = 0; i < ARRAYSIZEOF(vids); ++i) {
    rowkeys[i].set_uint64(vids[i]);
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(
        ObIvfRowkeyDistEntry(ObRowkey(&rowkeys[i], 1), distances[i])));
  }
  ASSERT_EQ(OB_SUCCESS, iter.collect_filtered_async_ivf_candidates(
      candidates, collector, collected_count));
  EXPECT_EQ(3, collected_count);
  ASSERT_EQ(OB_SUCCESS, iter.append_async_ivf_topk(collector));
  ASSERT_EQ(3, iter.saved_rowkeys_.count());
  const uint64_t expected_vids[] = {20, 40, 30};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_vids); ++i) {
    EXPECT_EQ(expected_vids[i], iter.saved_rowkeys_.at(i).get_obj_ptr()[0].get_uint64());
  }
}


TEST_F(TestDASAsyncIVFScanIter, partition_snapshot_rebind_preserves_sql_context)
{
  SCN sql_scn;
  SCN first_scn;
  SCN second_scn;
  ASSERT_EQ(OB_SUCCESS, sql_scn.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, first_scn.convert_for_tx(120));
  ASSERT_EQ(OB_SUCCESS, second_scn.convert_for_tx(80));
  transaction::ObTxReadSnapshot sql_snapshot;
  sql_snapshot.init_special_read(sql_scn);
  sql_snapshot.core_.tx_id_ = transaction::ObTransID(42);
  sql_snapshot.core_.scn_ = transaction::ObTxSEQ(7, 0);
  transaction::ObTxReadSnapshot async_snapshot;
  ObDASIVFAsyncIter iter;
  iter.sql_snapshot_ = &sql_snapshot;
  iter.snapshot_ = &sql_snapshot;
  iter.async_read_snapshot_ = &async_snapshot;
  // Model a retained post-filter scan parameter from an earlier partition.
  iter.data_filter_iter_first_scan_ = false;
  transaction::ObTxReadSnapshot *lookup_snapshot = iter.get_async_read_snapshot();
  ASSERT_EQ(OB_SUCCESS, iter.prepare_async_read_snapshot(first_scn));
  EXPECT_EQ(lookup_snapshot, iter.snapshot_);
  EXPECT_EQ(120, lookup_snapshot->version().get_val_for_tx());
  EXPECT_EQ(120, iter.data_filter_scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(sql_snapshot.core_.tx_id_, lookup_snapshot->core_.tx_id_);
  EXPECT_EQ(sql_snapshot.core_.scn_, lookup_snapshot->core_.scn_);
  EXPECT_EQ(sql_snapshot.source_, lookup_snapshot->source_);
  EXPECT_EQ(300, sql_snapshot.version().get_val_for_tx());

  iter.reset_async_read_state();
  EXPECT_EQ(&sql_snapshot, iter.snapshot_);
  EXPECT_FALSE(lookup_snapshot->is_valid());
  ASSERT_EQ(OB_SUCCESS, iter.prepare_async_read_snapshot(second_scn));
  EXPECT_EQ(lookup_snapshot, iter.snapshot_);
  EXPECT_EQ(80, lookup_snapshot->version().get_val_for_tx());
  EXPECT_EQ(80, iter.data_filter_scan_param_.snapshot_.version().get_val_for_tx());
  EXPECT_EQ(300, sql_snapshot.version().get_val_for_tx());
  iter.reset_async_read_state();
}

TEST_F(TestDASAsyncIVFScanIter, reuse_rebuilds_owned_segment_groups_at_new_snapshot)
{
  SCN sql_scn;
  ASSERT_EQ(OB_SUCCESS, sql_scn.convert_for_tx(300));
  transaction::ObTxReadSnapshot sql_snapshot;
  sql_snapshot.init_special_read(sql_scn);
  transaction::ObTxReadSnapshot async_snapshot;
  ObDASIVFAsyncIter iter;
  iter.sql_snapshot_ = &sql_snapshot;
  iter.snapshot_ = &sql_snapshot;
  iter.async_read_snapshot_ = &async_snapshot;
  // Seed the storage result directly: this DAS fixture has no tenant tablet service.
  storage::ObAsyncIndexSegmentGroup *group = OB_NEWx(
      storage::ObAsyncIndexSegmentGroup, &iter.segment_group_ctx_.allocator_);
  ASSERT_NE(nullptr, group);
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = 120;
  ASSERT_EQ(OB_SUCCESS, group->init(1));
  ASSERT_EQ(OB_SUCCESS, group->add_version_range(0, range));
  ASSERT_EQ(OB_SUCCESS, iter.segment_group_ctx_.groups_.push_back(group));
  ASSERT_EQ(OB_SUCCESS, iter.segment_group_ctx_.async_index_read_snapshot_.convert_for_tx(120));
  ASSERT_EQ(1, iter.segment_group_ctx_.get_group_count());
  ASSERT_EQ(OB_SUCCESS, iter.prepare_async_read_snapshot(iter.segment_group_ctx_.get_async_index_read_snapshot()));
  EXPECT_EQ(120, async_snapshot.version().get_val_for_tx());
  iter.partition_scan_finished_ = true;

  ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
  EXPECT_EQ(0, iter.segment_group_ctx_.get_group_count());
  EXPECT_FALSE(iter.segment_group_ctx_.get_async_index_read_snapshot().is_valid());
  EXPECT_FALSE(async_snapshot.is_valid());
  EXPECT_FALSE(iter.partition_scan_finished_);
  EXPECT_EQ(&sql_snapshot, iter.snapshot_);

  // The next partition can have an older publication SCN. No old ranges survive.
  group = OB_NEWx(storage::ObAsyncIndexSegmentGroup, &iter.segment_group_ctx_.allocator_);
  ASSERT_NE(nullptr, group);
  range.snapshot_version_ = 80;
  ASSERT_EQ(OB_SUCCESS, group->init(1));
  ASSERT_EQ(OB_SUCCESS, group->add_version_range(0, range));
  ASSERT_EQ(OB_SUCCESS, iter.segment_group_ctx_.groups_.push_back(group));
  ASSERT_EQ(OB_SUCCESS, iter.segment_group_ctx_.async_index_read_snapshot_.convert_for_tx(80));
  ASSERT_EQ(OB_SUCCESS, iter.prepare_async_read_snapshot(iter.segment_group_ctx_.get_async_index_read_snapshot()));
  ASSERT_EQ(1, iter.segment_group_ctx_.get_group_count());
  EXPECT_EQ(80, iter.segment_group_ctx_.get_group(0)->get_tablet_version_ranges(0).at(0).snapshot_version_);
  EXPECT_EQ(80, async_snapshot.version().get_val_for_tx());
  EXPECT_EQ(300, sql_snapshot.version().get_val_for_tx());
  ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
}

TEST_F(TestDASAsyncIVFScanIter, profile_util_keeps_partition_and_segments_separate)
{
  ObOpProfile<ObMetric> parent(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  ObDASIVFAsyncIter iter;
  {
    ObProfileSwitcher switcher(&parent);
    iter.bind_partition_profile();
  }
  ASSERT_EQ(&parent, iter.get_partition_profile());
  ASSERT_NE(nullptr, iter.profile_);
  ObOpProfile<ObMetric> *first =
      ObIVFProfileUtil::register_child(iter.profile_, ObProfileId::VECTOR_SEARCH_IVF_SEGMENT);
  ObOpProfile<ObMetric> *second =
      ObIVFProfileUtil::register_child(iter.profile_, ObProfileId::VECTOR_SEARCH_IVF_SEGMENT);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  ASSERT_NE(first, second);
  storage::ObIvfSSTableScanIter first_iter(allocator_);
  storage::ObIvfSSTableScanIter second_iter(allocator_);
  first_iter.set_profile(first);
  second_iter.set_profile(second);
  // Neither the current TLS node nor a sibling Segment may receive these writes.
  ObOpProfile<ObMetric> unrelated(ObProfileId::PHY_TABLE_SCAN, &allocator_);
  ObProfileSwitcher switcher(&unrelated);
  ObIVFProfileUtil::inc(iter.get_partition_profile(), ObMetricId::VEC_ITER_FILTER_ROW, 7);
  ObIVFProfileUtil::inc(first_iter.get_profile(), ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT, 3);
  ObIVFProfileUtil::inc(second_iter.get_profile(), ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT, 5);
  ObIVFProfileUtil::set_once(first_iter.get_profile(),
                             ObMetricId::VEC_IVF_FIRST_LOW_PRECISION_VECTOR_SCAN_CID, 2);
  ObIVFProfileUtil::set_once(first_iter.get_profile(),
                             ObMetricId::VEC_IVF_FIRST_LOW_PRECISION_VECTOR_SCAN_CID, 9);
  ASSERT_NE(nullptr, parent.get_metric(ObMetricId::VEC_ITER_FILTER_ROW));
  EXPECT_EQ(7, parent.get_metric(ObMetricId::VEC_ITER_FILTER_ROW)->value());
  ASSERT_NE(nullptr, first->get_metric(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT));
  ASSERT_NE(nullptr, second->get_metric(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT));
  EXPECT_EQ(3, first->get_metric(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT)->value());
  EXPECT_EQ(5, second->get_metric(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT)->value());
  ASSERT_NE(nullptr, first->get_metric(ObMetricId::VEC_IVF_FIRST_LOW_PRECISION_VECTOR_SCAN_CID));
  EXPECT_EQ(2, first->get_metric(ObMetricId::VEC_IVF_FIRST_LOW_PRECISION_VECTOR_SCAN_CID)->value());
  EXPECT_EQ(nullptr, parent.get_metric(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT));
  EXPECT_EQ(nullptr, first->get_metric(ObMetricId::VEC_ITER_FILTER_ROW));
  first_iter.reuse();
  iter.clear_partition_profile();
  ObIVFProfileUtil::inc(first_iter.get_profile(), ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT, 11);
  ObIVFProfileUtil::inc(iter.get_partition_profile(), ObMetricId::VEC_ITER_FILTER_ROW, 11);
  EXPECT_EQ(3, first->get_metric(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT)->value());
  EXPECT_EQ(7, parent.get_metric(ObMetricId::VEC_ITER_FILTER_ROW)->value());
  EXPECT_EQ(nullptr, unrelated.get_metric(ObMetricId::VEC_ITER_FILTER_ROW));
  EXPECT_EQ(nullptr, unrelated.get_metric(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT));
  EXPECT_EQ(&unrelated, get_current_profile());
}

TEST_F(TestDASAsyncIVFScanIter, post_filter_profile_accumulates_batches_and_rounds)
{
  verify_post_filter_profile(false);
  verify_post_filter_profile(true);
}

TEST_F(TestDASAsyncIVFScanIter, post_filter_point_get_is_limited_to_disk_bbq)
{
  lib::MemoryContext mem_context;
  lib::ContextParam context_param;
  context_param.set_mem_attr(MTL_ID(), "AsyncIVFTest", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, CURRENT_CONTEXT->CREATE_CONTEXT(mem_context, context_param));
  DEFER(DESTROY_CONTEXT(mem_context));
  const ObVectorIndexAlgorithmType algorithms[] = {
      VIAT_DISK_BBQ, VIAT_IVF_FLAT, VIAT_IVF_SQ8, VIAT_IVF_PQ, VIAT_IPIVF, VIAT_IPIVF_SQ};
  for (const auto algorithm : algorithms) {
    for (const bool is_vectorized : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "algorithm=" << algorithm
                                      << ", vectorized=" << is_vectorized);
      ObExecContext exec_ctx(allocator_);
      ObEvalCtx eval_ctx(exec_ctx);
      ObDASVecAuxScanCtDef vec_ctdef(allocator_);
      ObDASVecAuxScanRtDef vec_rtdef;
      ObDASScanCtDef ctdef(allocator_);
      ObDASScanRtDef rtdef;
      ObPushdownOperator pd_op(eval_ctx, ctdef.pd_expr_spec_);
      transaction::ObTxReadSnapshot snapshot;
      SCN scn;
      ObDASIVFAsyncIter iter;
      MockAsyncIVFPostFilterScanIter scan;
      ObIVFRowkeyDistMap candidates;
      ObSEArray<ObIvfRowkeyDistEntry, 16> matched;
      ObObj keys[3];
      ASSERT_EQ(OB_SUCCESS, candidates.create(32, ObMemAttr(MTL_ID(), "AsyncIVFTest")));
      for (int64_t i = 0; i < ARRAYSIZEOF(keys); ++i) {
        keys[i].set_uint64(i + 1);
        const ObRowkey rowkey(&keys[i], 1);
        ASSERT_EQ(OB_SUCCESS, candidates.set_refactored(rowkey, ObIvfRowkeyDistEntry(rowkey, i)));
      }
      vec_ctdef.algorithm_type_ = algorithm;
      vec_rtdef.eval_ctx_ = &eval_ctx;
      ctdef.ref_table_id_ = 50001;
      rtdef.p_pd_expr_op_ = &pd_op;
      ASSERT_EQ(OB_SUCCESS, scn.convert_for_tx(100));
      snapshot.specify_snapshot_scn(scn);
      iter.mem_context_ = mem_context;
      iter.snapshot_ = &snapshot;
      iter.ls_id_ = ObLSID(1);
      iter.vec_aux_ctdef_ = &vec_ctdef;
      iter.vec_aux_rtdef_ = &vec_rtdef;
      iter.data_filter_tablet_id_ = ObTabletID(1001);
      iter.data_filter_ctdef_ = &ctdef;
      iter.data_filter_rtdef_ = &rtdef;
      iter.data_filter_iter_ = &scan;

      // Exercise initialization and the next round's rescan through the real parameter builder.
      for (int64_t round = 0; round < 2; ++round) {
        ASSERT_EQ(OB_SUCCESS, iter.do_table_post_filter(is_vectorized, candidates, matched));
        EXPECT_EQ(VIAT_DISK_BBQ == algorithm, scan.get_scan_param().is_get_);
        EXPECT_EQ(VIAT_DISK_BBQ == algorithm ? static_cast<ObIAllocator *>(&iter.disk_bbq_lookup_alloc_)
                                          : &mem_context->get_arena_allocator(),
                  scan.get_scan_param().scan_allocator_);
        EXPECT_EQ(ARRAYSIZEOF(keys), scan.last_key_count_);
        EXPECT_EQ(1, scan.scan_count_);
        EXPECT_EQ(round, scan.rescan_count_);
        EXPECT_TRUE(matched.empty());
      }

      // Auxiliary scans retain the default SCAN path even for DiskBBQ.
      bool first_scan = true;
      ASSERT_EQ(OB_SUCCESS, iter.do_aux_table_scan(first_scan, iter.data_filter_scan_param_,
          &ctdef, &rtdef, &scan, iter.data_filter_tablet_id_));
      EXPECT_FALSE(scan.get_scan_param().is_get_);
      EXPECT_EQ(&mem_context->get_arena_allocator(), scan.get_scan_param().scan_allocator_);
      iter.data_filter_iter_ = nullptr;
      iter.mem_context_ = nullptr;
    }
  }
}

TEST_F(TestDASAsyncIVFScanIter, post_filter_malloc_batches_release_without_resetting_cross_round_state)
{
  lib::MemoryContext mem_context;
  lib::ContextParam context_param;
  context_param.set_mem_attr(MTL_ID(), "AsyncIVFTest", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, CURRENT_CONTEXT->CREATE_CONTEXT(mem_context, context_param));
  DEFER(DESTROY_CONTEXT(mem_context));
  for (const bool is_vectorized : {false, true}) {
    ObExecContext exec_ctx(allocator_);
    ObEvalCtx eval_ctx(exec_ctx);
    ObDASVecAuxScanCtDef vec_ctdef(allocator_);
    ObDASVecAuxScanRtDef vec_rtdef;
    ObDASScanCtDef ctdef(allocator_);
    ObDASScanRtDef rtdef;
    ObPushdownOperator pd_op(eval_ctx, ctdef.pd_expr_spec_);
    transaction::ObTxReadSnapshot snapshot;
    SCN scn;
    ObDASIVFAsyncIter iter;
    MockAsyncIVFAllocatingPostFilterScanIter scan;
    ObIVFRowkeyDistMap candidates;
    ObSEArray<ObIvfRowkeyDistEntry, 16> matched;
    blocksstable::ObIvfKnnSearchResult input;
    blocksstable::ObIvfTopKCollector collector;
    ASSERT_EQ(OB_SUCCESS, candidates.create(32, ObMemAttr(MTL_ID(), "AsyncIVFTest")));
    ASSERT_EQ(OB_SUCCESS, collector.init(3, iter.vec_op_alloc_));
    ASSERT_EQ(OB_SUCCESS, collector.push(7, 42, 0.5));
    for (int64_t i = 0; i < 2 * ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE + 7; ++i) {
      blocksstable::ObIvfKnnEntry entry;
      entry.vid_ = i + 1;
      ASSERT_EQ(OB_SUCCESS, input.add(entry));
    }
    char *persistent = static_cast<char *>(mem_context->get_arena_allocator().alloc(64));
    ASSERT_NE(nullptr, persistent);
    MEMSET(persistent, 0x5a, 64);
    const int64_t arena_used = mem_context->arena_used();
    const int64_t malloc_used = mem_context->malloc_used();
    vec_ctdef.algorithm_type_ = VIAT_DISK_BBQ;
    vec_rtdef.eval_ctx_ = &eval_ctx;
    ctdef.ref_table_id_ = 50001;
    rtdef.p_pd_expr_op_ = &pd_op;
    ASSERT_EQ(OB_SUCCESS, scn.convert_for_tx(100));
    snapshot.specify_snapshot_scn(scn);
    iter.mem_context_ = mem_context;
    iter.snapshot_ = &snapshot;
    iter.ls_id_ = ObLSID(1);
    iter.vec_aux_ctdef_ = &vec_ctdef;
    iter.vec_aux_rtdef_ = &vec_rtdef;
    iter.data_filter_tablet_id_ = ObTabletID(1001);
    iter.data_filter_ctdef_ = &ctdef;
    iter.data_filter_rtdef_ = &rtdef;
    iter.data_filter_iter_ = &scan;
    DEFER(iter.data_filter_iter_ = nullptr; iter.mem_context_ = nullptr);
    for (int64_t round = 0; round < 32; ++round) {
      candidates.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.add_async_ivf_filter_candidates(input, candidates));
      ASSERT_EQ(OB_SUCCESS, iter.do_table_post_filter(is_vectorized, candidates, matched));
      EXPECT_EQ(malloc_used, mem_context->malloc_used());
      EXPECT_EQ(arena_used, mem_context->arena_used());
      EXPECT_EQ(0, scan.range_.get_rowkeys().count());
      EXPECT_EQ(0, iter.disk_bbq_lookup_alloc_.used());
      EXPECT_EQ(nullptr, iter.disk_bbq_lookup_alloc_.head_);
      EXPECT_TRUE(collector.contains(42));
      for (int64_t i = 0; i < 64; ++i) { ASSERT_EQ(0x5a, persistent[i]); }
      for (const auto &entry : candidates) {
        EXPECT_GE(entry.first.get_obj_ptr()[0].get_uint64(), 1);
      }
    }
    // Fail after materialization, on both initial open and rescan. Cleanup must
    // return live bytes to baseline while retaining the original error.
    for (const bool first_scan : {false, true}) {
      for (const int failure : {OB_TIMEOUT, OB_CANCELED, OB_ALLOCATE_MEMORY_FAILED}) {
        iter.data_filter_iter_first_scan_ = first_scan;
        scan.open_ret_ = failure;
        scan.reuse_ret_ = OB_ERR_UNEXPECTED;
        EXPECT_EQ(failure, iter.do_table_post_filter(is_vectorized, candidates, matched));
        EXPECT_GT(mem_context->malloc_used(), malloc_used);
        EXPECT_GT(iter.disk_bbq_lookup_alloc_.used(), 0);
        ASSERT_FALSE(scan.range_.get_rowkeys().empty());
        EXPECT_GE(scan.range_.get_rowkeys().at(0).datums_[0].get_uint64(), 1);
        EXPECT_EQ(0x5a, static_cast<char *>(scan.abandoned_)[0]);
        EXPECT_TRUE(collector.contains(42));
        EXPECT_EQ(arena_used, mem_context->arena_used());
        // Release pending readers before the private owner can reclaim them.
        scan.reuse_ret_ = OB_SUCCESS;
        ASSERT_EQ(OB_SUCCESS, scan.reuse());
        iter.disk_bbq_lookup_alloc_.reuse();
        EXPECT_EQ(malloc_used, mem_context->malloc_used());
      }
    }
    scan.open_ret_ = OB_SUCCESS;
    scan.reuse_ret_ = OB_SUCCESS;
    for (const int failure : {OB_TIMEOUT, OB_CANCELED}) {
      iter.data_filter_iter_first_scan_ = true;
      scan.read_ret_ = failure;
      EXPECT_EQ(failure, iter.do_table_post_filter(is_vectorized, candidates, matched));
      EXPECT_EQ(malloc_used, mem_context->malloc_used());
      EXPECT_EQ(arena_used, mem_context->arena_used());
      EXPECT_TRUE(collector.contains(42));
    }
    EXPECT_EQ(32 * 3 + 14, scan.reuse_count_);
  }
}

TEST_F(TestDASAsyncIVFScanIter, disk_bbq_batch_reclaims_unowned_datum_after_append_failure)
{
  ObMalloc malloc_allocator(ObMemAttr(MTL_ID(), "AsyncIVFTest"));
  AsyncIVFFailAllocator allocations(malloc_allocator, -1);
  ObDASIvfBaseScanIter::DiskBBQLookupAllocator datums;
  datums.bind(allocations);
  AsyncIVFFailAllocator array_storage(malloc_allocator, 1);
  storage::ObTableScanRange range;
  using Rowkeys = decltype(range.rowkeys_);
  range.rowkeys_.~Rowkeys();
  new (&range.rowkeys_) Rowkeys(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(array_storage));
  ObObj keys[16];
  ObSEArray<ObNewRange, 16> ranges;
  for (int64_t i = 0; i < ARRAYSIZEOF(keys); ++i) {
    keys[i].set_uint64(i + 1);
    ObNewRange key_range;
    key_range.start_key_.assign(&keys[i], 1);
    key_range.end_key_.assign(&keys[i], 1);
    key_range.border_flag_.set_inclusive_start();
    key_range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(key_range));
  }
  storage::ObTablet tablet;
  ObQueryFlag scan_flag;
  range.allocator_ = &datums;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, range.init_rowkeys(tablet, false, ranges, scan_flag, nullptr));
  EXPECT_EQ(1, array_storage.attempts_);
  EXPECT_EQ(range.rowkeys_.count() + 1, allocations.attempts_);
  // The unmodified common path leaves its last converted key unowned.
  EXPECT_EQ(range.rowkeys_.count() + 1, allocations.allocations_.size());
  range.reset();
  EXPECT_EQ(1, allocations.allocations_.size());
  datums.reuse();
  EXPECT_TRUE(allocations.allocations_.empty());
  EXPECT_TRUE(array_storage.allocations_.empty());
}

TEST_F(TestDASAsyncIVFScanIter, disk_bbq_owner_reclaims_unmodified_memtable_mget_allocations)
{
  ObTenantDataVersionMgr::get_instance().set_mock_data_version(DATA_CURRENT_VERSION);
  DEFER(ObTenantDataVersionMgr::get_instance().set_mock_data_version(0));
  lib::MemoryContext mem_context;
  lib::ContextParam context_param;
  context_param.set_mem_attr(MTL_ID(), "AsyncIVFTest", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, CURRENT_CONTEXT->CREATE_CONTEXT(mem_context, context_param));
  DEFER(DESTROY_CONTEXT(mem_context));
  memtable::ObMemtable table;
  ObSEArray<blocksstable::ObDatumRowkey, 1> rowkeys;
  for (const int64_t column_count : {5, 32}) {
    ObSEArray<share::schema::ObColDesc, 32> columns;
    for (int64_t i = 0; i < column_count; ++i) {
      share::schema::ObColDesc column;
      column.col_id_ = OB_APP_MIN_COLUMN_ID + i;
      column.col_type_.set_type(ObIntType);
      ASSERT_EQ(OB_SUCCESS, columns.push_back(column));
    }
    storage::ObTableReadInfo read_info;
    ASSERT_EQ(OB_SUCCESS, read_info.init(allocator_, column_count, 1, false, columns, nullptr));
    storage::ObTableIterParam param;
    param.read_info_ = &read_info;
    ObDASIvfBaseScanIter::DiskBBQLookupAllocator lookup;
    lookup.bind(mem_context->get_malloc_allocator());
    storage::ObTableAccessContext context;
    context.allocator_ = &lookup;
    context.stmt_allocator_ = &allocator_;
    memtable::ObMemtableMGetIterator mget;
    const int64_t baseline = mem_context->malloc_used();
    for (int64_t round = 0; round < 32; ++round) {
      ASSERT_EQ(OB_SUCCESS, mget.init(param, context, &table, &rowkeys));
      const blocksstable::ObDatumRow *row = nullptr;
      EXPECT_EQ(OB_ITER_END, mget.inner_get_next_row(row));
      mget.reuse();
      EXPECT_GT(lookup.used(), 0); // Common MGet still leaves ColumnMap behind.
      lookup.reuse();
      EXPECT_EQ(baseline, mem_context->malloc_used());
      mget.reset();
      EXPECT_EQ(baseline, mem_context->malloc_used());
    }
    if (column_count == 32) {
      AsyncIVFFailAllocator failed(mem_context->get_malloc_allocator(), 1);
      lookup.reset();
      lookup.bind(failed);
      EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, mget.init(param, context, &table, &rowkeys));
      mget.reset();
      lookup.reset();
      EXPECT_TRUE(failed.allocations_.empty());
      EXPECT_EQ(baseline, mem_context->malloc_used());
    }
  }
}

TEST_F(TestDASAsyncIVFScanIter, disk_bbq_batch_owner_supports_free_alignment_and_allocation_failure)
{
  ObMalloc malloc_allocator(ObMemAttr(MTL_ID(), "AsyncIVFTest"));
  AsyncIVFFailAllocator backing(malloc_allocator, 3);
  ObDASIvfBaseScanIter::DiskBBQLookupAllocator lookup;
  lookup.bind(backing);
  void *first = lookup.alloc(17);
  void *aligned = lookup.alloc_align(33, 64);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, aligned);
  EXPECT_EQ(0, reinterpret_cast<uintptr_t>(aligned) % 64);
  EXPECT_EQ(nullptr, lookup.alloc(1));
  EXPECT_EQ(2, backing.allocations_.size());
  lookup.free(first);
  lookup.free_align(aligned);
  EXPECT_EQ(0, lookup.used());
  EXPECT_TRUE(backing.allocations_.empty());
  ASSERT_NE(nullptr, lookup.alloc(19));
  ASSERT_NE(nullptr, lookup.alloc(23));
  lookup.reuse();
  EXPECT_EQ(0, lookup.used());
  EXPECT_TRUE(backing.allocations_.empty());
  lookup.reset();
  EXPECT_EQ(nullptr, lookup.alloc(1));
}

TEST_F(TestDASAsyncIVFScanIter, profile_wrapper_rescan_keeps_same_tablet_executions)
{
  ObOpProfile<ObMetric> sql_profile(ObProfileId::PHY_TABLE_SCAN, &allocator_);
  ObProfileSwitcher switcher(&sql_profile);
  ObDASVecAuxScanCtDef ctdef(allocator_);
  float query_vector[2] = {1.0F, 0.0F};
  ObExecContext exec_ctx(allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  ObExpr rowkey_expr;
  ObExpr query_expr;
  ObExpr limit_expr;
  ObExpr distance_expr;
  ObDASSortCtDef sort_ctdef(allocator_);
  ObDASSortRtDef sort_rtdef;
  ObDASIVFAsyncIter iter;
  MockAsyncIVFLookupScanIter input;
  // Rescan must refresh a valid query before reaching the read-preparation failure.
  ASSERT_EQ(OB_SUCCESS, init_vector_lookup_exprs(eval_ctx, query_vector, sizeof(query_vector),
                                               rowkey_expr, query_expr, limit_expr, distance_expr));
  ASSERT_EQ(OB_SUCCESS, sort_ctdef.sort_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort_ctdef.sort_exprs_.push_back(&distance_expr));
  sort_rtdef.eval_ctx_ = &eval_ctx;
  ctdef.dim_ = ARRAYSIZEOF(query_vector);
  iter.dim_ = ctdef.dim_;
  iter.sort_ctdef_ = &sort_ctdef;
  iter.sort_rtdef_ = &sort_rtdef;
  iter.search_vec_ = &query_expr;
  iter.real_search_vec_.assign_ptr(reinterpret_cast<char *>(query_vector), sizeof(query_vector));
  iter.vec_aux_ctdef_ = &ctdef;
  ASSERT_EQ(OB_SUCCESS, iter.init_filter_input(&input));
  ObDASProfileIter wrapper;
  ObDASProfileIterParam param(&iter, true, ObTabletID(1001), ObProfileId::VECTOR_SEARCH);
  ASSERT_EQ(OB_SUCCESS, wrapper.inner_init(param));
  ObDASIter *children[] = {&iter};
  wrapper.children_ = children;
  wrapper.children_cnt_ = 1;
  // Preparation fails before storage access, but the wrapper preserves both executions.
  ASSERT_EQ(OB_INVALID_ARGUMENT, wrapper.do_table_scan());
  ObOpProfile<ObMetric> *first = wrapper.execution_profile_;
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, first->get_metric(ObMetricId::VEC_IVF_FINISH_TAG));
  EXPECT_EQ(2, first->get_metric(ObMetricId::VEC_IVF_FINISH_TAG)->value());
  ASSERT_EQ(OB_INVALID_ARGUMENT, wrapper.rescan());
  ASSERT_EQ(sizeof(query_vector), iter.real_search_vec_.length());
  EXPECT_NE(reinterpret_cast<char *>(query_vector), iter.real_search_vec_.ptr());
  ASSERT_NE(first, wrapper.execution_profile_);
  EXPECT_EQ(2, wrapper.execution_profile_->get_metric(ObMetricId::VEC_IVF_FINISH_TAG)->value());
  EXPECT_EQ(2, first->get_metric(ObMetricId::VEC_IVF_FINISH_TAG)->value());
  ASSERT_NE(nullptr, sql_profile.get_child_head());
  ObOpProfile<ObMetric> *root = sql_profile.get_child_head()->elem_;
  ASSERT_NE(nullptr, root->get_child_head()->next_);
  EXPECT_EQ(nullptr, root->get_child_head()->next_->next_);
  EXPECT_EQ(&sql_profile, get_current_profile());
  wrapper.children_ = nullptr;
  wrapper.children_cnt_ = 0;
}

TEST_F(TestDASAsyncIVFScanIter, profile_preserves_tree_estimate_until_bound)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  ObDASIVFAsyncIter iter;
  iter.vec_aux_ctdef_ = &ctdef;
  iter.vec_index_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  // Tree construction selects the path before the wrapper creates its Profile.
  ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 256, 129));
  EXPECT_EQ(129, iter.profile_est_rows_);
  EXPECT_EQ(256, iter.profile_est_total_rows_);
  ObOpProfile<ObMetric> parent(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  ObProfileSwitcher switcher(&parent);
  iter.bind_partition_profile();
  ASSERT_NE(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_ROWS));
  ASSERT_NE(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY));
  EXPECT_EQ(129, parent.get_metric(ObMetricId::VEC_IVF_EST_ROWS)->value());
  EXPECT_EQ(50, parent.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY)->value());
  EXPECT_EQ(nullptr, parent.get_metric(ObMetricId::VEC_TOTAL_SEARCH_TIME));
  ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 100000, 25000));
  ASSERT_NE(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_ROWS));
  ASSERT_NE(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY));
  EXPECT_EQ(25000, parent.get_metric(ObMetricId::VEC_IVF_EST_ROWS)->value());
  EXPECT_EQ(25, parent.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY)->value());
}

TEST_F(TestDASAsyncIVFScanIter, profile_estimates_clear_on_reuse_and_invalid_estimate)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  ObDASIVFAsyncIter iter;
  iter.vec_aux_ctdef_ = &ctdef;
  iter.vec_index_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 256, 129));
  ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
  EXPECT_EQ(0, iter.profile_est_rows_);
  EXPECT_EQ(0, iter.profile_est_total_rows_);
  ObOpProfile<ObMetric> next(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  {
    ObProfileSwitcher switcher(&next);
    iter.bind_partition_profile();
    ASSERT_NE(nullptr, next.get_metric(ObMetricId::VEC_IVF_EST_ROWS));
    ASSERT_NE(nullptr, next.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY));
    EXPECT_EQ(0, next.get_metric(ObMetricId::VEC_IVF_EST_ROWS)->value());
    EXPECT_EQ(0, next.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY)->value());
    ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 256, 64));
    EXPECT_EQ(64, next.get_metric(ObMetricId::VEC_IVF_EST_ROWS)->value());
    EXPECT_EQ(25, next.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY)->value());
  }
  iter.clear_partition_profile();
  ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, 256, 129));
  ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_TABLET_NOT_EXIST, -1, -1));
  EXPECT_EQ(0, iter.profile_est_rows_);
  EXPECT_EQ(0, iter.profile_est_total_rows_);
  ObOpProfile<ObMetric> fallback(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  {
    ObProfileSwitcher switcher(&fallback);
    iter.bind_partition_profile();
    ASSERT_NE(nullptr, fallback.get_metric(ObMetricId::VEC_IVF_EST_ROWS));
    ASSERT_NE(nullptr, fallback.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY));
    EXPECT_EQ(0, fallback.get_metric(ObMetricId::VEC_IVF_EST_ROWS)->value());
    EXPECT_EQ(0, fallback.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY)->value());
  }
}

TEST_F(TestDASAsyncIVFScanIter, profile_estimate_zero_denominator_and_clamped_selectivity)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  ObDASIVFAsyncIter iter;
  iter.vec_aux_ctdef_ = &ctdef;
  iter.vec_index_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  const int64_t partition_rows[] = {0, 256, 256};
  const int64_t filter_rows[] = {0, 0, 300};
  const int64_t expected_selectivity[] = {0, 0, 100};
  for (int64_t i = 0; i < ARRAYSIZEOF(partition_rows); ++i) {
    iter.clear_partition_profile();
    ASSERT_EQ(OB_SUCCESS, iter.apply_partition_filter_estimate(OB_SUCCESS, partition_rows[i], filter_rows[i]));
    ObOpProfile<ObMetric> parent(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
    ObProfileSwitcher switcher(&parent);
    iter.bind_partition_profile();
    ASSERT_NE(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_ROWS));
    EXPECT_EQ(filter_rows[i], parent.get_metric(ObMetricId::VEC_IVF_EST_ROWS)->value());
    ASSERT_NE(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY));
    EXPECT_EQ(expected_selectivity[i], parent.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY)->value());
    iter.clear_partition_profile();
  }
}

TEST_F(TestDASAsyncIVFScanIter, profile_omits_estimates_for_fixed_and_hinted_paths)
{
  ObDASIVFAsyncIter iter;
  ObOpProfile<ObMetric> parent(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  ObProfileSwitcher switcher(&parent);
  iter.vec_index_type_ = ObVecIndexType::VEC_INDEX_PRE;
  iter.bind_partition_profile();
  EXPECT_EQ(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_ROWS));
  EXPECT_EQ(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY));
  iter.clear_partition_profile();
  iter.vec_index_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  iter.is_adaptive_specify_by_hint_ = true;
  iter.bind_partition_profile();
  EXPECT_EQ(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_ROWS));
  EXPECT_EQ(nullptr, parent.get_metric(ObMetricId::VEC_IVF_EST_SELECTIVITY));
}

TEST_F(TestDASAsyncIVFScanIter, profile_early_close_and_finish_tags)
{
  ObOpProfile<ObMetric> parent(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  ObDASIVFAsyncIter iter;
  {
    ObProfileSwitcher switcher(&parent);
    iter.bind_partition_profile();
    iter.record_profile_finish(OB_SUCCESS);
    EXPECT_EQ(nullptr, parent.get_metric(ObMetricId::VEC_IVF_FINISH_TAG));
    iter.clear_partition_profile();
    iter.clear_partition_profile();
    EXPECT_EQ(nullptr, parent.get_metric(ObMetricId::VEC_IVF_FINISH_TAG));
    EXPECT_EQ(nullptr, iter.profile_);
    EXPECT_EQ(nullptr, iter.get_partition_profile());
  }
  ObOpProfile<ObMetric> next(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  {
    ObProfileSwitcher switcher(&next);
    iter.bind_partition_profile();
    EXPECT_EQ(nullptr, next.get_metric(ObMetricId::VEC_IVF_EST_ROWS));
    iter.record_profile_finish(OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY);
    EXPECT_EQ(1, next.get_metric(ObMetricId::VEC_IVF_FINISH_TAG)->value());
    iter.record_profile_finish(OB_ITER_END);
    EXPECT_EQ(1, next.get_metric(ObMetricId::VEC_IVF_FINISH_TAG)->value());
    iter.record_profile_finish(OB_CANCELED);
    EXPECT_EQ(2, next.get_metric(ObMetricId::VEC_IVF_FINISH_TAG)->value());
  }
}

TEST_F(TestDASAsyncIVFScanIter, profile_allocation_failure_does_not_change_query_error)
{
  AsyncIVFFailAllocator fail_allocator(allocator_, 1);
  ObOpProfile<ObMetric> parent(ObProfileId::VECTOR_SEARCH_VEC_ITER, &fail_allocator);
  ObProfileSwitcher switcher(&parent);
  ObDASIVFAsyncIter iter;
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.do_table_scan());
  EXPECT_EQ(nullptr, iter.profile_);
  // Metric writes use a local diagnostic return code even when registration fails.
  EXPECT_GT(fail_allocator.attempts_, 0);
}

TEST_F(TestDASAsyncIVFScanIter, profile_wrapper_allocation_failure_keeps_child_alive)
{
  ObExecContext exec_ctx(allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASScanCtDef ctdef(allocator_);
  MockAsyncIVFLookupScanIter child;
  child.eval_ctx_ = &eval_ctx;
  child.exec_ctx_ = &exec_ctx;
  child.output_ = &ctdef.result_output_;
  child.max_size_ = 1;
  child.inited_ = true;
  // Fail either the wrapper allocation or its children array.
  for (int64_t fail_at = 1; fail_at <= 2; ++fail_at) {
    AsyncIVFFailAllocator fail_allocator(allocator_, fail_at);
    ObDASIter *wrapper = nullptr;
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObDASIterUtils::create_profile_iter(
        fail_allocator, ObTabletID(1001), &child, true, ObProfileId::VECTOR_SEARCH, wrapper));
    EXPECT_EQ(nullptr, wrapper);
    // The wrapper remains in the arena after its resources have been released.
    ASSERT_EQ(static_cast<size_t>(fail_at - 1), fail_allocator.allocations_.size());
    if (fail_at == 2) {
      auto *released_wrapper = static_cast<ObDASIter *>(fail_allocator.allocations_.back());
      EXPECT_FALSE(released_wrapper->is_inited());
      EXPECT_EQ(DAS_ITER_INVALID, released_wrapper->get_type());
    }
    EXPECT_TRUE(child.inited_);
  }
}

TEST_F(TestDASAsyncIVFScanIter, hybrid_ivf_rescan_creates_profile_and_rebinds_snapshots)
{
  ObDASScanOp scan_op(allocator_);
  ObDASSearchCtx search_ctx(allocator_, scan_op);
  ObDASAsyncIndexQueryCtx async_query_ctx;
  ObAsyncIndexPartitionSegCtx partition;
  search_ctx.async_query_ctx_ = &async_query_ctx;
  search_ctx.set_current_async_partition(&partition);
  SCN sql_scn;
  ASSERT_EQ(OB_SUCCESS, sql_scn.convert_for_tx(300));
  transaction::ObTxReadSnapshot sql_snapshot;
  sql_snapshot.init_special_read(sql_scn);
  ObDASVecIndexIVFScanIter iter;
  iter.search_ctx_ = &search_ctx;
  iter.snapshot_ = &sql_snapshot;
  iter.centroid_iter_first_scan_ = false;
  iter.cid_vec_iter_first_scan_ = false;
  iter.com_aux_iter_first_scan_ = false;
  iter.special_aux_iter_first_scan_ = false;
  ObOpProfile<ObMetric> parent(ObProfileId::HYBRID_SEARCH_PARTITION, &allocator_);
  ObProfileSwitcher switcher(&parent);
  ObOpProfile<ObMetric> *previous_profile = nullptr;
  const int64_t snapshots[] = {120, 80};
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, partition.storage_group_ctx_.async_index_read_snapshot_.convert_for_tx(
        snapshots[i]));
    ASSERT_EQ(OB_SUCCESS, iter.rescan());
    ASSERT_NE(nullptr, iter.profile_);
    EXPECT_NE(previous_profile, iter.profile_);
    EXPECT_EQ(&parent, iter.profile_->get_parent());
    EXPECT_EQ(snapshots[i], iter.centroid_scan_param_.snapshot_.version().get_val_for_tx());
    EXPECT_EQ(snapshots[i], iter.cid_vec_scan_param_.snapshot_.version().get_val_for_tx());
    EXPECT_EQ(snapshots[i], iter.com_aux_scan_param_.snapshot_.version().get_val_for_tx());
    EXPECT_EQ(snapshots[i], iter.special_aux_scan_param_.snapshot_.version().get_val_for_tx());
    EXPECT_EQ(300, sql_snapshot.version().get_val_for_tx());
    EXPECT_EQ(&parent, get_current_profile());
    previous_profile = iter.profile_;
  }
  search_ctx.set_current_async_partition(nullptr);
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.rescan());
  EXPECT_EQ(80, iter.centroid_scan_param_.snapshot_.version().get_val_for_tx());
}

TEST_F(TestDASAsyncIVFScanIter, profile_snapshot_tracks_each_async_read_view)
{
  SCN sql_scn;
  ASSERT_EQ(OB_SUCCESS, sql_scn.convert_for_tx(300));
  transaction::ObTxReadSnapshot sql_snapshot;
  sql_snapshot.init_special_read(sql_scn);
  transaction::ObTxReadSnapshot async_snapshot;
  ObDASIVFAsyncIter iter;
  iter.sql_snapshot_ = &sql_snapshot;
  iter.snapshot_ = &sql_snapshot;
  iter.async_read_snapshot_ = &async_snapshot;
  ObOpProfile<ObMetric> first(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  ObOpProfile<ObMetric> second(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  SCN published_scn;
  {
    ObProfileSwitcher switcher(&first);
    iter.bind_partition_profile();
    ASSERT_EQ(OB_SUCCESS, published_scn.convert_for_tx(120));
    ASSERT_EQ(OB_SUCCESS, iter.prepare_async_read_snapshot(published_scn));
    EXPECT_EQ(120, first.get_metric(ObMetricId::VEC_IVF_SCN)->value());
    iter.partition_scan_finished_ = true;
    iter.record_profile_finish(OB_SUCCESS);
    EXPECT_EQ(0, first.get_metric(ObMetricId::VEC_IVF_FINISH_TAG)->value());
    ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
  }
  {
    ObProfileSwitcher switcher(&second);
    iter.bind_partition_profile();
    ASSERT_EQ(OB_SUCCESS, published_scn.convert_for_tx(80));
    ASSERT_EQ(OB_SUCCESS, iter.prepare_async_read_snapshot(published_scn));
    EXPECT_EQ(80, second.get_metric(ObMetricId::VEC_IVF_SCN)->value());
    EXPECT_EQ(120, first.get_metric(ObMetricId::VEC_IVF_SCN)->value());
    EXPECT_EQ(300, sql_snapshot.version().get_val_for_tx());
    EXPECT_EQ(nullptr, second.get_metric(ObMetricId::VEC_IVF_FINISH_TAG));
    ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
  }
}

TEST_F(TestDASAsyncIVFScanIter, failed_read_preparation_stops_scan_and_rescan)
{
  ObDASVecAuxScanCtDef ctdef(allocator_);
  float query_vector[2] = {1.0F, 0.0F};
  ObExecContext exec_ctx(allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  ObExpr rowkey_expr;
  ObExpr query_expr;
  ObExpr limit_expr;
  ObExpr distance_expr;
  ObDASSortCtDef sort_ctdef(allocator_);
  ObDASSortRtDef sort_rtdef;
  ObDASIVFAsyncIter iter;
  ObDASLocalLookupIter filter_lookup;
  MockAsyncIVFLookupScanIter input;
  ObTableScanParam scan_param;
  input.set_scan_param(scan_param);
  filter_lookup.index_table_iter_ = &input;
  // Keep the query state valid so the missing read state remains the failure under test.
  ASSERT_EQ(OB_SUCCESS, init_vector_lookup_exprs(eval_ctx, query_vector, sizeof(query_vector),
                                               rowkey_expr, query_expr, limit_expr, distance_expr));
  ASSERT_EQ(OB_SUCCESS, sort_ctdef.sort_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, sort_ctdef.sort_exprs_.push_back(&distance_expr));
  sort_rtdef.eval_ctx_ = &eval_ctx;
  ctdef.dim_ = ARRAYSIZEOF(query_vector);
  iter.dim_ = ctdef.dim_;
  iter.sort_ctdef_ = &sort_ctdef;
  iter.sort_rtdef_ = &sort_rtdef;
  iter.search_vec_ = &query_expr;
  iter.real_search_vec_.assign_ptr(reinterpret_cast<char *>(query_vector), sizeof(query_vector));
  iter.vec_aux_ctdef_ = &ctdef;
  ASSERT_EQ(OB_SUCCESS, iter.init_filter_input(&filter_lookup));
  ctdef.is_async_index_ = true;
  ctdef.vec_type_ = ObVecIndexType::VEC_INDEX_PRE;

  // A missing SQL snapshot must fail at scan entry, before scalar input opens.
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.do_table_scan());
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.rescan());
  ASSERT_EQ(sizeof(query_vector), iter.real_search_vec_.length());
  EXPECT_NE(reinterpret_cast<char *>(query_vector), iter.real_search_vec_.ptr());
  ctdef.vec_type_ = ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.do_table_scan());
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.rescan());
  EXPECT_EQ(0, input.scan_count_);
  EXPECT_EQ(0, input.rescan_count_);
  EXPECT_EQ(0, iter.segment_group_ctx_.get_group_count());
}

TEST_F(TestDASAsyncIVFScanIter, rejects_snapshot_newer_than_sql_without_rebinding)
{
  SCN sql_scn;
  SCN future_scn;
  ASSERT_EQ(OB_SUCCESS, sql_scn.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, future_scn.convert_for_tx(301));
  transaction::ObTxReadSnapshot sql_snapshot;
  sql_snapshot.init_special_read(sql_scn);
  transaction::ObTxReadSnapshot async_snapshot;
  ObDASIVFAsyncIter iter;
  iter.sql_snapshot_ = &sql_snapshot;
  iter.snapshot_ = &sql_snapshot;
  iter.async_read_snapshot_ = &async_snapshot;
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.prepare_async_read_snapshot(future_scn));
  EXPECT_EQ(&sql_snapshot, iter.snapshot_);
  EXPECT_FALSE(iter.get_async_read_snapshot()->is_valid());
  EXPECT_EQ(300, sql_snapshot.version().get_val_for_tx());
}

TEST_F(TestDASAsyncIVFScanIter, lookup_rescan_observes_rebound_async_snapshot)
{
  ObDASVecAuxScanCtDef index_ctdef(allocator_);
  ObDASVecAuxScanRtDef index_rtdef;
  index_ctdef.is_async_index_ = true;
  index_rtdef.ctdef_ = &index_ctdef;
  transaction::ObTxReadSnapshot snapshot;
  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_tx(120));
  snapshot.init_special_read(scn);
  snapshot.core_.tx_id_ = transaction::ObTransID(42);
  snapshot.core_.scn_ = transaction::ObTxSEQ(7, 0);
  ObDASLocalLookupIter lookup;
  MockAsyncIVFLookupScanIter scan;
  lookup.is_first_lookup_ = false;
  lookup.index_ctdef_ = &index_ctdef;
  lookup.index_rtdef_ = &index_rtdef;
  lookup.snapshot_ = &snapshot;
  lookup.data_table_iter_ = &scan;
  scan.set_scan_param(lookup.get_lookup_param());

  ASSERT_EQ(OB_SUCCESS, lookup.do_index_lookup());
  EXPECT_EQ(120, scan.observed_snapshot_);
  EXPECT_EQ(snapshot.core_.tx_id_, lookup.get_lookup_param().snapshot_.core_.tx_id_);
  EXPECT_EQ(snapshot.core_.scn_, lookup.get_lookup_param().snapshot_.core_.scn_);
  snapshot.reset();
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_tx(80));
  snapshot.init_special_read(scn);
  ASSERT_EQ(OB_SUCCESS, lookup.do_index_lookup());
  EXPECT_EQ(80, scan.observed_snapshot_);
  EXPECT_EQ(2, scan.rescan_count_);

  scan.rescan_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, lookup.do_index_lookup());
  EXPECT_EQ(3, scan.rescan_count_);
}

TEST_F(TestDASAsyncIVFScanIter, ordinary_lookup_rescan_keeps_initialized_snapshot)
{
  ObDASScanCtDef index_ctdef(allocator_);
  ObDASScanRtDef index_rtdef;
  index_rtdef.ctdef_ = &index_ctdef;
  transaction::ObTxReadSnapshot snapshot;
  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_tx(120));
  snapshot.init_special_read(scn);
  ObDASLocalLookupIter lookup;
  MockAsyncIVFLookupScanIter scan;
  lookup.is_first_lookup_ = false;
  lookup.index_ctdef_ = &index_ctdef;
  lookup.index_rtdef_ = &index_rtdef;
  lookup.snapshot_ = &snapshot;
  lookup.data_table_iter_ = &scan;
  scan.set_scan_param(lookup.get_lookup_param());
  ASSERT_EQ(OB_SUCCESS, lookup.get_lookup_param().snapshot_.assign(snapshot));
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_tx(80));
  snapshot.specify_snapshot_scn(scn);

  ASSERT_EQ(OB_SUCCESS, lookup.do_index_lookup());
  EXPECT_EQ(120, scan.observed_snapshot_);
  EXPECT_EQ(1, scan.rescan_count_);
}

TEST_F(TestDASAsyncIVFScanIter, missing_async_snapshot_stops_lookup_rescan)
{
  ObDASVecAuxScanCtDef index_ctdef(allocator_);
  ObDASVecAuxScanRtDef index_rtdef;
  index_ctdef.is_async_index_ = true;
  index_rtdef.ctdef_ = &index_ctdef;
  ObDASLocalLookupIter lookup;
  MockAsyncIVFLookupScanIter scan;
  lookup.is_first_lookup_ = false;
  lookup.index_ctdef_ = &index_ctdef;
  lookup.index_rtdef_ = &index_rtdef;
  lookup.data_table_iter_ = &scan;
  scan.set_scan_param(lookup.get_lookup_param());

  EXPECT_EQ(OB_ERR_UNEXPECTED, lookup.do_index_lookup());
  EXPECT_EQ(0, scan.rescan_count_);
}


// --- Hybrid-search DiskBBQ (async IVF) profile -------------------------------
// The hybrid search vector branch hangs HYBRID_SEARCH_VEC_ASYNC_IVF_SCAN under
// the Hybrid Search Vector Iter and owns one HYBRID_SEARCH_IVF_SEGMENT node per
// visible range. These cases cover the ambient-profile gate and the
// non-merging structure without needing a storage environment.

TEST_F(TestDASAsyncIVFScanIter, hybrid_async_ivf_profile_gate_accepts_only_hybrid_vector_iter)
{
  ObOpProfile<ObMetric> hybrid_iter(ObProfileId::HYBRID_SEARCH_VEC_ITER, &allocator_);
  ObOpProfile<ObMetric> plain_iter(ObProfileId::VECTOR_SEARCH_VEC_ITER, &allocator_);
  ObOpProfile<ObMetric> hybrid_partition(ObProfileId::HYBRID_SEARCH_PARTITION, &allocator_);
  ObOpProfile<ObMetric> table_scan(ObProfileId::PHY_TABLE_SCAN, &allocator_);
  {
    ObProfileSwitcher switcher(&hybrid_iter);
    EXPECT_EQ(&hybrid_iter, ObDASVecIndexAsyncIVFScanIter::get_current_hybrid_vector_iter_profile());
  }
  {
    // The plain SQL vector family must never receive hybrid scan nodes.
    ObProfileSwitcher switcher(&plain_iter);
    EXPECT_EQ(nullptr, ObDASVecIndexAsyncIVFScanIter::get_current_hybrid_vector_iter_profile());
  }
  {
    ObProfileSwitcher switcher(&hybrid_partition);
    EXPECT_EQ(nullptr, ObDASVecIndexAsyncIVFScanIter::get_current_hybrid_vector_iter_profile());
  }
  {
    ObProfileSwitcher switcher(&table_scan);
    EXPECT_EQ(nullptr, ObDASVecIndexAsyncIVFScanIter::get_current_hybrid_vector_iter_profile());
  }
  // ObProfileSwitcher restores the ambient (unset) profile once it leaves scope.
  EXPECT_EQ(nullptr, get_current_profile());
}

TEST_F(TestDASAsyncIVFScanIter, hybrid_async_ivf_scan_and_segment_nodes_stay_distinct)
{
  // Both ids sit inside [HYBRID_SEARCH_BEGIN, HYBRID_SEARCH_END], so merge and
  // persist keep the scan node as a non-aggregating parent and preserve every
  // Segment instance without extra special-casing.
  ASSERT_TRUE(ObProfileUtil::is_hybrid_search_profile(ObProfileId::HYBRID_SEARCH_VEC_ASYNC_IVF_SCAN));
  ASSERT_TRUE(ObProfileUtil::is_hybrid_search_profile(ObProfileId::HYBRID_SEARCH_IVF_SEGMENT));
  ASSERT_FALSE(ObProfileUtil::is_hybrid_search_profile(ObProfileId::VECTOR_SEARCH_VEC_ASYNC_IVF_SCAN));

  ObOpProfile<ObMetric> vector_iter(ObProfileId::HYBRID_SEARCH_VEC_ITER, &allocator_);
  ObOpProfile<ObMetric> *scan = nullptr;
  {
    ObProfileSwitcher switcher(&vector_iter);
    scan = ObIVFProfileUtil::register_child(&vector_iter, ObProfileId::HYBRID_SEARCH_VEC_ASYNC_IVF_SCAN);
  }
  ASSERT_NE(nullptr, scan);
  EXPECT_EQ(ObProfileId::HYBRID_SEARCH_VEC_ASYNC_IVF_SCAN, scan->get_id());
  EXPECT_EQ(&vector_iter, scan->get_parent());

  ObOpProfile<ObMetric> *first = ObIVFProfileUtil::register_child(scan, ObProfileId::HYBRID_SEARCH_IVF_SEGMENT);
  ObOpProfile<ObMetric> *second = ObIVFProfileUtil::register_child(scan, ObProfileId::HYBRID_SEARCH_IVF_SEGMENT);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_NE(first, second);
  EXPECT_EQ(ObProfileId::HYBRID_SEARCH_IVF_SEGMENT, first->get_id());
  EXPECT_EQ(ObProfileId::HYBRID_SEARCH_IVF_SEGMENT, second->get_id());

  // Per-range identity must stay on its own node, not be merged by profile id.
  ObIVFProfileUtil::set(first, ObMetricId::VEC_IVF_VERSION_RANGE_BASE, 100);
  ObIVFProfileUtil::set(second, ObMetricId::VEC_IVF_VERSION_RANGE_BASE, 200);
  ObIVFProfileUtil::set(first, ObMetricId::VEC_IVF_VERSION_RANGE_SNAPSHOT, 101);
  ObIVFProfileUtil::set(second, ObMetricId::VEC_IVF_VERSION_RANGE_SNAPSHOT, 201);
  ASSERT_NE(nullptr, first->get_metric(ObMetricId::VEC_IVF_VERSION_RANGE_BASE));
  ASSERT_NE(nullptr, second->get_metric(ObMetricId::VEC_IVF_VERSION_RANGE_BASE));
  EXPECT_EQ(100, first->get_metric(ObMetricId::VEC_IVF_VERSION_RANGE_BASE)->value());
  EXPECT_EQ(200, second->get_metric(ObMetricId::VEC_IVF_VERSION_RANGE_BASE)->value());
  EXPECT_EQ(101, first->get_metric(ObMetricId::VEC_IVF_VERSION_RANGE_SNAPSHOT)->value());
  EXPECT_EQ(201, second->get_metric(ObMetricId::VEC_IVF_VERSION_RANGE_SNAPSHOT)->value());

  // The scan node owns the epoch counters; the Vector Iter must stay clean.
  ObIVFProfileUtil::set(scan, ObMetricId::VEC_IVF_SEGMENT_COUNT, 2);
  ObIVFProfileUtil::set(scan, ObMetricId::VEC_IVF_IS_BRUTE_FORCE, false);
  ASSERT_NE(nullptr, scan->get_metric(ObMetricId::VEC_IVF_SEGMENT_COUNT));
  EXPECT_EQ(2, scan->get_metric(ObMetricId::VEC_IVF_SEGMENT_COUNT)->value());
  EXPECT_EQ(nullptr, vector_iter.get_metric(ObMetricId::VEC_IVF_SEGMENT_COUNT));
  EXPECT_EQ(nullptr, vector_iter.get_metric(ObMetricId::VEC_IVF_IS_BRUTE_FORCE));
  EXPECT_EQ(nullptr, first->get_metric(ObMetricId::VEC_IVF_SEGMENT_COUNT));
}

TEST_F(TestDASAsyncIVFScanIter, hybrid_async_ivf_records_only_while_bound)
{
  // clear_partition_profile() style null-safety: recording without a bound scan
  // node must be a silent no-op rather than a crash or a stray TLS write.
  ObOpProfile<ObMetric> unrelated(ObProfileId::PHY_TABLE_SCAN, &allocator_);
  ObOpProfile<ObMetric> *empty_profile = nullptr;
  ObProfileSwitcher switcher(&unrelated);
  ObIVFProfileUtil::set(empty_profile, ObMetricId::VEC_IVF_SEGMENT_COUNT, 3);
  ObIVFProfileUtil::inc(empty_profile, ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT, 1);
  EXPECT_EQ(nullptr, unrelated.get_metric(ObMetricId::VEC_IVF_SEGMENT_COUNT));
  EXPECT_EQ(nullptr, unrelated.get_metric(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT));
  EXPECT_EQ(&unrelated, get_current_profile());
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

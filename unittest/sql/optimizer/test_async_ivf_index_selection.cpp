/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "share/schema/ob_table_schema.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_factory.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_vec_define.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_select_log_plan.h"

namespace oceanbase
{
namespace sql
{

using namespace common;
using namespace share;

TEST(TestAsyncIVFIndexSelection, create_optimizer_objects)
{
  const uint64_t data_table_id = 50001;
  const uint64_t async_ivf_index_tid = 60001;
  const uint64_t async_delete_index_tid = 60002;
  ObArenaAllocator allocator("AsyncIVFSel");
  schema::ObTableSchema data_schema;
  data_schema.set_table_id(data_table_id);
  ASSERT_EQ(OB_SUCCESS, data_schema.add_simple_index_info(schema::ObAuxTableMetaInfo(
      async_ivf_index_tid, schema::USER_INDEX, schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL)));
  ASSERT_EQ(OB_SUCCESS, data_schema.add_simple_index_info(schema::ObAuxTableMetaInfo(
      async_delete_index_tid, schema::USER_INDEX, schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL)));

  schema::ObTableSchema index_schema;
  index_schema.set_table_id(async_ivf_index_tid);
  index_schema.set_data_table_id(data_table_id);
  index_schema.set_index_type(schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  ASSERT_EQ(OB_SUCCESS,
      index_schema.set_index_params(ObString("TYPE=DISK_BBQ,DISTANCE=L2")));

  AccessPath access_path(data_table_id,
                         data_table_id,
                         async_ivf_index_tid,
                         nullptr,
                         ObOrderDirection::UNORDERED,
                         allocator);

  ASSERT_EQ(OB_SUCCESS, access_path.alloc_vector_index_info_if_need(allocator));
  ASSERT_NE(nullptr, access_path.vec_idx_info_);
  ASSERT_EQ(OB_SUCCESS,
      access_path.vec_idx_info_->vec_index_ids_.push_back(async_ivf_index_tid));
  ASSERT_EQ(OB_SUCCESS, access_path.vec_idx_info_->vec_extra_info_.set_vec_param_info(
      &index_schema));
  access_path.vec_idx_info_->inited_ = true;

  ASSERT_TRUE(access_path.vec_idx_info_->has_vec_index());
  ASSERT_TRUE(access_path.vec_idx_info_->vec_extra_info_.is_async_ivf_scan());
  ASSERT_EQ(1, access_path.vec_idx_info_->vec_index_ids_.count());
  ASSERT_EQ(async_ivf_index_tid, access_path.vec_idx_info_->vec_index_ids_.at(0));

  VecIndexAccessInfo copied_access_info;
  ASSERT_EQ(OB_SUCCESS, copied_access_info.assign(*access_path.vec_idx_info_));
  ASSERT_TRUE(copied_access_info.has_vec_index());
  ASSERT_TRUE(copied_access_info.vec_extra_info_.is_async_ivf_scan());
  ASSERT_EQ(1, copied_access_info.vec_index_ids_.count());
  ASSERT_EQ(async_ivf_index_tid, copied_access_info.vec_index_ids_.at(0));

  ObVecIndexInfo logical_info(allocator);
  logical_info.vector_index_param_ = copied_access_info.vec_extra_info_.vector_index_param_;
  logical_info.set_async_ivf_index(copied_access_info);
  ASSERT_EQ(OB_SUCCESS, logical_info.aux_table_id_.push_back(async_ivf_index_tid));
  ASSERT_EQ(OB_SUCCESS, logical_info.aux_table_id_.push_back(async_delete_index_tid));
  ASSERT_TRUE(logical_info.is_async_ivf_scan());
  ASSERT_EQ(async_ivf_index_tid,
            logical_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX));
  ASSERT_EQ(async_delete_index_tid,
            logical_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX));
  ASSERT_TRUE(logical_info.is_vec_aux_table_id(async_ivf_index_tid));
  ASSERT_TRUE(logical_info.is_vec_aux_table_id(async_delete_index_tid));
}

TEST(TestAsyncIVFIndexSelection, initialize_async_ivf_access_info_in_main_flow)
{
  const uint64_t data_table_id = 50001;
  const uint64_t async_ivf_index_tid = 60001;

  schema::ObTableSchema index_schema;
  index_schema.set_table_id(async_ivf_index_tid);
  index_schema.set_data_table_id(data_table_id);
  index_schema.set_index_type(schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  ASSERT_EQ(OB_SUCCESS,
      index_schema.set_index_params(ObString("TYPE=DISK_BBQ,DISTANCE=L2")));

  VecIndexAccessInfo access_info;
  ASSERT_EQ(OB_SUCCESS, access_info.vec_index_ids_.push_back(async_ivf_index_tid));
  ASSERT_EQ(OB_SUCCESS, access_info.vec_extra_info_.set_vec_param_info(&index_schema));
  ASSERT_FALSE(access_info.has_vec_index());
  ASSERT_TRUE(access_info.vec_extra_info_.is_async_index_);
  ASSERT_EQ(1, access_info.vec_index_ids_.count());
  ASSERT_EQ(async_ivf_index_tid, access_info.vec_index_ids_.at(0));
}

TEST(TestAsyncIVFIndexSelection, choose_async_ivf_adaptive_path_at_boundaries)
{
  struct AdaptivePathCase
  {
    const char *name_;
    int64_t row_count_;
    double selectivity_;
    bool can_use_vec_pri_opt_;
    ObVecIdxAdaTryPath expected_path_;
  };
  const AdaptivePathCase cases[] = {
      {"zero filtered rows", 100000, 0.0, false,
       ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER},
      {"exact brute force upper bound", 10000, 1.0, false,
       ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER},
      {"above brute force upper bound", 10001, 1.0, false,
       ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER},
      {"rowkey optimization at 1 percent", 2000000, 0.01, true,
       ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER},
      {"rowkey optimization at 90 percent", 100000, 0.90, true,
       ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER},
      {"rowkey optimization above 90 percent", 100000, 0.9001, true,
       ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER},
      {"non-rowkey optimization at 10 percent", 1000000, 0.10, false,
       ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER},
      {"non-rowkey optimization above 10 percent", 1000000, 0.1001, false,
       ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER},
      {"non-rowkey optimization below prefilter row limit", 4999990, 0.10, false,
       ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER},
      {"non-rowkey optimization at prefilter row limit", 5000000, 0.10, false,
       ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER},
  };
  const int64_t case_count = static_cast<int64_t>(sizeof(cases) / sizeof(cases[0]));
  ObVecIdxExtraInfo vec_info;
  vec_info.is_async_index_ = true;
  vec_info.vector_index_param_.type_ = ObVectorIndexAlgorithmType::VIAT_DISK_BBQ;

  for (int64_t i = 0; i < case_count; ++i) {
    vec_info.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN;
    vec_info.set_row_count(cases[i].row_count_);
    vec_info.set_selectivity(cases[i].selectivity_);
    vec_info.set_can_use_vec_pri_opt(cases[i].can_use_vec_pri_opt_);

    ASSERT_EQ(OB_SUCCESS,
              ObVectorIndexUtil::set_adaptive_try_path(vec_info, nullptr, false, false)) << cases[i].name_;
    EXPECT_EQ(cases[i].expected_path_, vec_info.adaptive_try_path_) << cases[i].name_;
  }
}

TEST(TestAsyncIVFIndexSelection, convert_async_ivf_pre_filter_to_in_filter)
{
  ObVecIdxExtraInfo vec_info;
  vec_info.is_async_index_ = true;
  vec_info.vector_index_param_.type_ = ObVectorIndexAlgorithmType::VIAT_DISK_BBQ;
  vec_info.set_row_count(100000);
  vec_info.set_selectivity(0.50);
  vec_info.set_can_use_vec_pri_opt(true);
  vec_info.with_extra_info_ = true;

  ASSERT_EQ(OB_SUCCESS,
            ObVectorIndexUtil::set_adaptive_try_path(vec_info, nullptr, true, false));
  EXPECT_EQ(ObVecIdxAdaTryPath::VEC_INDEX_IN_FILTER, vec_info.adaptive_try_path_);

  ObVecIdxExtraInfo non_rowkey_vec_info;
  non_rowkey_vec_info.is_async_index_ = true;
  non_rowkey_vec_info.vector_index_param_.type_ = ObVectorIndexAlgorithmType::VIAT_DISK_BBQ;
  non_rowkey_vec_info.set_row_count(10000);
  non_rowkey_vec_info.set_selectivity(1.0);
  non_rowkey_vec_info.with_extra_info_ = true;

  ASSERT_EQ(OB_SUCCESS,
            ObVectorIndexUtil::set_adaptive_try_path(non_rowkey_vec_info, nullptr, false, false));
  EXPECT_EQ(ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER,
            non_rowkey_vec_info.adaptive_try_path_);
}

TEST(TestAsyncIVFIndexSelection, preserve_optimizer_selected_adaptive_path)
{
  ObVecIdxExtraInfo vec_info;
  vec_info.is_async_index_ = true;
  vec_info.vector_index_param_.type_ = ObVectorIndexAlgorithmType::VIAT_DISK_BBQ;
  vec_info.set_row_count(100000);
  vec_info.set_selectivity(1.0);
  vec_info.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER;

  ASSERT_EQ(OB_SUCCESS,
            ObVectorIndexUtil::set_adaptive_try_path(vec_info, nullptr, true, false));
  EXPECT_EQ(ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER, vec_info.adaptive_try_path_);

  vec_info.set_selectivity(0.0);
  vec_info.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
  ASSERT_EQ(OB_SUCCESS,
            ObVectorIndexUtil::set_adaptive_try_path(vec_info, nullptr, false, false));
  EXPECT_EQ(ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER, vec_info.adaptive_try_path_);
}

TEST(TestAsyncIVFIndexSelection, collect_async_ivf_related_table_ids)
{
  const uint64_t data_table_id = 50001;
  const uint64_t async_ivf_index_tid = 60001;
  const uint64_t async_delete_index_tid = 60002;
  ObArenaAllocator allocator("AsyncIVFLoc");
  schema::ObTableSchema data_schema;
  data_schema.set_table_id(data_table_id);
  ASSERT_EQ(OB_SUCCESS, data_schema.add_simple_index_info(schema::ObAuxTableMetaInfo(
      async_ivf_index_tid, schema::USER_INDEX, schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL)));
  ASSERT_EQ(OB_SUCCESS, data_schema.add_simple_index_info(schema::ObAuxTableMetaInfo(
      async_delete_index_tid, schema::USER_INDEX, schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL)));
  ObGlobalHint global_hint(allocator);
  ObAddr server;
  ObRawExprFactory expr_factory(allocator);
  ObOptimizerContext optimizer_ctx(nullptr,
                                   nullptr,
                                   nullptr,
                                   nullptr,
                                   allocator,
                                   nullptr,
                                   server,
                                   nullptr,
                                   global_hint,
                                   expr_factory,
                                   nullptr,
                                   false,
                                   nullptr);
  ObSelectLogPlan plan(optimizer_ctx, nullptr);
  ObLogTableScan table_scan(plan);
  TableLocRelInfo rel_info(allocator);

  table_scan.set_table_id(data_table_id);
  table_scan.set_ref_table_id(data_table_id);
  table_scan.set_index_table_id(async_ivf_index_tid);
  ASSERT_EQ(OB_SUCCESS, table_scan.alloc_vector_index_info_if_need());
  ObVecIndexInfo *vec_info = table_scan.get_vector_index_info();
  ASSERT_NE(nullptr, vec_info);
  vec_info->main_table_tid_ = data_table_id;
  vec_info->is_async_index_ = true;
  vec_info->vector_index_param_.type_ = ObVectorIndexAlgorithmType::VIAT_DISK_BBQ;

  ASSERT_EQ(OB_SUCCESS, plan.prepare_async_ivf_vector_index_scan(
      data_schema, async_ivf_index_tid, &table_scan));
  ASSERT_NE(nullptr, vec_info->async_delete_info_);
  ASSERT_EQ(async_ivf_index_tid,
            vec_info->get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX));
  ASSERT_EQ(async_delete_index_tid,
            vec_info->get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX));
  ASSERT_EQ(OB_SUCCESS, plan.collect_vec_index_location_related_info(table_scan, rel_info));
  ASSERT_EQ(3, rel_info.related_ids_.count());
  EXPECT_EQ(async_ivf_index_tid, rel_info.related_ids_.at(0));
  EXPECT_EQ(async_delete_index_tid, rel_info.related_ids_.at(1));
  EXPECT_EQ(data_table_id, rel_info.related_ids_.at(2));
}

TEST(TestAsyncIVFIndexSelection, async_ivf_requires_main_table_rowkey)
{
  ObArenaAllocator allocator("AsyncIVFRowkey");
  ObGlobalHint global_hint(allocator);
  ObAddr server;
  ObRawExprFactory expr_factory(allocator);
  ObOptimizerContext optimizer_ctx(nullptr,
                                   nullptr,
                                   nullptr,
                                   nullptr,
                                   allocator,
                                   nullptr,
                                   server,
                                   nullptr,
                                   global_hint,
                                   expr_factory,
                                   nullptr,
                                   false,
                                   nullptr);
  ObSelectLogPlan plan(optimizer_ctx, nullptr);
  ObLogTableScan table_scan(plan);

  EXPECT_FALSE(table_scan.need_get_rowkey_exprs());
  ASSERT_EQ(OB_SUCCESS, table_scan.alloc_vector_index_info_if_need());
  ASSERT_NE(nullptr, table_scan.get_vector_index_info());
  table_scan.get_vector_index_info()->is_async_index_ = true;
  table_scan.get_vector_index_info()->vector_index_param_.type_ =
      ObVectorIndexAlgorithmType::VIAT_DISK_BBQ;
  EXPECT_TRUE(table_scan.need_get_rowkey_exprs());
}

TEST(TestAsyncIVFIndexSelection, async_ivf_ctdef_tree_round_trip)
{
  const uint64_t data_table_id = 50001;
  const uint64_t async_ivf_index_tid = 60001;
  const uint64_t async_delete_index_tid = 60002;
  ObArenaAllocator allocator("AsyncIVFCtDef");
  ObDASTableLookupCtDef *lookup_ctdef = nullptr;
  ObDASSortCtDef *sort_ctdef = nullptr;
  ObDASVecAuxScanCtDef *vec_scan_ctdef = nullptr;
  ObDASScanCtDef *filter_input_ctdef = nullptr;
  ObDASScanCtDef *ivf_scan_ctdef = nullptr;
  ObDASScanCtDef *delete_scan_ctdef = nullptr;
  ObDASScanCtDef *lookup_scan_ctdef = nullptr;
  ObDASVecAuxScanRtDef *vec_scan_rtdef = nullptr;
  ObDASScanRtDef *filter_input_rtdef = nullptr;
  ObDASScanRtDef *ivf_scan_rtdef = nullptr;
  ObDASScanRtDef *delete_scan_rtdef = nullptr;
  const int64_t async_ivf_child_count = 3;
  ObDASBaseRtDef *vec_child_rtdefs[async_ivf_child_count] = {nullptr, nullptr, nullptr};

  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_TABLE_LOOKUP, allocator, lookup_ctdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_SORT, allocator, sort_ctdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_VEC_SCAN, allocator, vec_scan_ctdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_TABLE_SCAN, allocator, filter_input_ctdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_TABLE_SCAN, allocator, ivf_scan_ctdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_TABLE_SCAN, allocator, delete_scan_ctdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_TABLE_SCAN, allocator, lookup_scan_ctdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_rtdef(
      DAS_OP_VEC_SCAN, allocator, vec_scan_rtdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_rtdef(
      DAS_OP_TABLE_SCAN, allocator, filter_input_rtdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_rtdef(
      DAS_OP_TABLE_SCAN, allocator, ivf_scan_rtdef));
  ASSERT_EQ(OB_SUCCESS, ObDASTaskFactory::alloc_das_rtdef(
      DAS_OP_TABLE_SCAN, allocator, delete_scan_rtdef));

  lookup_ctdef->children_cnt_ = 2;
  lookup_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef *, &allocator, 2);
  sort_ctdef->children_cnt_ = 1;
  sort_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef *, &allocator, 1);
  vec_scan_ctdef->children_cnt_ = async_ivf_child_count;
  vec_scan_ctdef->children_ = OB_NEW_ARRAY(
      ObDASBaseCtDef *, &allocator, async_ivf_child_count);
  ASSERT_NE(nullptr, lookup_ctdef->children_);
  ASSERT_NE(nullptr, sort_ctdef->children_);
  ASSERT_NE(nullptr, vec_scan_ctdef->children_);

  lookup_ctdef->children_[0] = sort_ctdef;
  lookup_ctdef->children_[1] = lookup_scan_ctdef;
  sort_ctdef->children_[0] = vec_scan_ctdef;
  vec_scan_ctdef->children_[0] = filter_input_ctdef;
  vec_scan_ctdef->children_[1] = ivf_scan_ctdef;
  vec_scan_ctdef->children_[2] = delete_scan_ctdef;
  vec_scan_ctdef->is_async_index_ = true;
  vec_scan_ctdef->algorithm_type_ = ObVectorIndexAlgorithmType::VIAT_DISK_BBQ;
  vec_scan_ctdef->dim_ = 3;
  vec_scan_ctdef->vec_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  vec_scan_ctdef->adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER;
  vec_scan_ctdef->is_adaptive_specify_by_hint_ = true;
  ivf_scan_ctdef->ref_table_id_ = async_ivf_index_tid;
  ivf_scan_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_VEC_ASYNC_IVF_INDEX_SCAN;
  delete_scan_ctdef->ref_table_id_ = async_delete_index_tid;
  delete_scan_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_ASYNC_INDEX_DELETE_SCAN;
  lookup_scan_ctdef->ref_table_id_ = data_table_id;
  vec_scan_rtdef->ctdef_ = vec_scan_ctdef;
  vec_scan_rtdef->children_cnt_ = async_ivf_child_count;
  vec_child_rtdefs[0] = filter_input_rtdef;
  vec_child_rtdefs[1] = ivf_scan_rtdef;
  vec_child_rtdefs[2] = delete_scan_rtdef;
  vec_scan_rtdef->children_ = vec_child_rtdefs;
  ASSERT_TRUE(vec_scan_ctdef->is_async_ivf_pre_filter_path());
  ASSERT_EQ(DAS_OP_VEC_SCAN, vec_scan_rtdef->op_type_);
  EXPECT_EQ(filter_input_ctdef, vec_scan_ctdef->get_inv_idx_scan_ctdef());
  EXPECT_EQ(filter_input_rtdef, vec_scan_rtdef->get_inv_idx_scan_rtdef());
  EXPECT_EQ(ivf_scan_rtdef, vec_scan_rtdef->get_async_ivf_index_scan_rtdef());
  EXPECT_EQ(delete_scan_rtdef, vec_scan_rtdef->get_async_delete_scan_rtdef());
  EXPECT_EQ(nullptr, vec_scan_rtdef->get_functional_lookup_rtdef());

  ObDASAttachSpec source_spec(allocator, nullptr);
  ObDASTableLocMeta *ivf_loc_meta =
      OB_NEWx(ObDASTableLocMeta, &allocator, allocator);
  ObDASTableLocMeta *delete_loc_meta =
      OB_NEWx(ObDASTableLocMeta, &allocator, allocator);
  ASSERT_NE(nullptr, ivf_loc_meta);
  ASSERT_NE(nullptr, delete_loc_meta);
  ivf_loc_meta->table_loc_id_ = data_table_id;
  ivf_loc_meta->ref_table_id_ = async_ivf_index_tid;
  delete_loc_meta->table_loc_id_ = data_table_id;
  delete_loc_meta->ref_table_id_ = async_delete_index_tid;
  ASSERT_EQ(OB_SUCCESS, source_spec.attach_loc_metas_.push_back(ivf_loc_meta));
  ASSERT_EQ(OB_SUCCESS, source_spec.attach_loc_metas_.push_back(delete_loc_meta));
  source_spec.attach_ctdef_ = lookup_ctdef;
  const int64_t serialize_size = source_spec.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(serialize_size));
  int64_t pos = 0;
  ASSERT_GT(serialize_size, 0);
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, source_spec.serialize(buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);

  ObArenaAllocator decoded_allocator("AsyncIVFDec");
  ObDASAttachSpec decoded_spec(decoded_allocator, nullptr);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_spec.deserialize(buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);
  ASSERT_NE(nullptr, decoded_spec.attach_ctdef_);
  EXPECT_NE(nullptr, decoded_spec.get_attach_loc_meta(data_table_id, async_ivf_index_tid));
  EXPECT_NE(nullptr, decoded_spec.get_attach_loc_meta(data_table_id, async_delete_index_tid));
  ASSERT_EQ(DAS_OP_TABLE_LOOKUP, decoded_spec.attach_ctdef_->op_type_);
  const ObDASTableLookupCtDef *decoded_lookup =
      static_cast<const ObDASTableLookupCtDef *>(decoded_spec.attach_ctdef_);
  ASSERT_EQ(2, decoded_lookup->children_cnt_);
  ASSERT_EQ(DAS_OP_SORT, decoded_lookup->children_[0]->op_type_);
  ASSERT_EQ(DAS_OP_TABLE_SCAN, decoded_lookup->children_[1]->op_type_);
  const ObDASSortCtDef *decoded_sort =
      static_cast<const ObDASSortCtDef *>(decoded_lookup->children_[0]);
  ASSERT_EQ(1, decoded_sort->children_cnt_);
  ASSERT_EQ(DAS_OP_VEC_SCAN, decoded_sort->children_[0]->op_type_);
  const ObDASVecAuxScanCtDef *decoded_vec_scan =
      static_cast<const ObDASVecAuxScanCtDef *>(decoded_sort->children_[0]);
  ASSERT_TRUE(decoded_vec_scan->is_async_index_);
  EXPECT_TRUE(decoded_vec_scan->is_adaptive_specify_by_hint_);
  ASSERT_EQ(async_ivf_child_count, decoded_vec_scan->children_cnt_);
  EXPECT_EQ(ObVectorIndexAlgorithmType::VIAT_DISK_BBQ, decoded_vec_scan->algorithm_type_);
  EXPECT_EQ(3, decoded_vec_scan->dim_);
  EXPECT_TRUE(decoded_vec_scan->is_async_ivf_pre_filter_path());
  ASSERT_NE(nullptr, decoded_vec_scan->get_async_ivf_index_scan_ctdef());
  ASSERT_NE(nullptr, decoded_vec_scan->get_async_delete_scan_ctdef());
  EXPECT_EQ(DAS_OP_TABLE_SCAN,
            decoded_vec_scan->get_async_ivf_index_scan_ctdef()->op_type_);
  EXPECT_EQ(DAS_OP_TABLE_SCAN,
            decoded_vec_scan->get_async_delete_scan_ctdef()->op_type_);
  EXPECT_EQ(ObTSCIRScanType::OB_VEC_ASYNC_IVF_INDEX_SCAN,
            decoded_vec_scan->get_async_ivf_index_scan_ctdef()->ir_scan_type_);
  EXPECT_EQ(ObTSCIRScanType::OB_ASYNC_INDEX_DELETE_SCAN,
            decoded_vec_scan->get_async_delete_scan_ctdef()->ir_scan_type_);
  EXPECT_EQ(nullptr, decoded_vec_scan->get_functional_lookup_ctdef());
  EXPECT_EQ(async_ivf_index_tid,
            decoded_vec_scan->get_async_ivf_index_scan_ctdef()->ref_table_id_);
  EXPECT_EQ(async_delete_index_tid,
            decoded_vec_scan->get_async_delete_scan_ctdef()->ref_table_id_);
}

TEST(TestAsyncIVFIndexSelection, preserve_async_ivf_iter_filter_path)
{
  ObArenaAllocator allocator("AsyncIVFPath");
  ObDASVecAuxScanCtDef vec_scan_ctdef(allocator);
  vec_scan_ctdef.is_async_index_ = true;
  vec_scan_ctdef.vec_type_ = ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;
  vec_scan_ctdef.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
  EXPECT_FALSE(vec_scan_ctdef.is_async_ivf_pre_filter_path());

  vec_scan_ctdef.vec_type_ = ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER;
  vec_scan_ctdef.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN;
  EXPECT_FALSE(vec_scan_ctdef.is_async_ivf_pre_filter_path());
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

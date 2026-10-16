/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/das/iter/ob_das_iter_utils.h"
#include "sql/das/iter/ob_das_vec_index_brute_scan_iter.h"
#include "sql/das/iter/ob_das_spiv_merge_iter.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_trans_define_v4.h"
#undef protected
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{
class CountingScan : public ObDASScanIter
{
public:
  CountingScan() : releases_(0), release_ret_(OB_SUCCESS) {}
  int releases_;
  int release_ret_;
protected:
  int inner_init(ObDASIterParam &) override { return OB_SUCCESS; }
  int inner_release() override { ++releases_; return release_ret_; }
};

class TestDASVectorBuildCleanup : public ::testing::Test
{
protected:
  TestDASVectorBuildCleanup()
      : alloc_("VecCleanupUT"), exec_ctx_(alloc_), eval_ctx_(exec_ctx_), output_(alloc_),
        ctdef_(alloc_), scan_ctdef_(alloc_)
  {}
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
    ObDASIterParam input_param(DAS_ITER_SCAN);
    input_param.eval_ctx_ = &eval_ctx_;
    input_param.exec_ctx_ = &exec_ctx_;
    input_param.output_ = &output_;
    ASSERT_EQ(OB_SUCCESS, input_.init(input_param));
    scan_ctdef_.ir_scan_type_ = ObTSCIRScanType::OB_VEC_BRUTE_MAIN_SCAN;
    children_[0] = &scan_ctdef_;
    ctdef_.children_ = children_;
    ctdef_.children_cnt_ = 1;
    param_.eval_ctx_ = &eval_ctx_;
    param_.exec_ctx_ = &exec_ctx_;
    param_.output_ = &output_;
    param_.ls_id_ = share::ObLSID(1);
    param_.tx_desc_ = &tx_desc_;
    param_.snapshot_ = &snapshot_;
    param_.main_table_iter_ = &input_;
    param_.vec_index_scan_ctdef_ = &ctdef_;
    param_.vec_index_scan_rtdef_ = &rtdef_;
    param_.sort_expr_ = &sort_expr_;
    ASSERT_TRUE(param_.is_valid());
  }
  void TearDown() override
  {
    if (input_.is_inited()) {
      EXPECT_EQ(OB_SUCCESS, input_.release());
    }
    EXPECT_EQ(1, input_.releases_);
  }

  void prepare_valid_output()
  {
    ASSERT_EQ(OB_SUCCESS, scan_ctdef_.result_output_.init(2));
    ASSERT_EQ(OB_SUCCESS, scan_ctdef_.result_output_.push_back(&vector_expr_));
    ASSERT_EQ(OB_SUCCESS, scan_ctdef_.result_output_.push_back(&vid_expr_));
    ASSERT_EQ(OB_SUCCESS, scan_ctdef_.access_column_ids_.init(2));
    ASSERT_EQ(OB_SUCCESS, scan_ctdef_.access_column_ids_.push_back(16));
    ASSERT_EQ(OB_SUCCESS, scan_ctdef_.access_column_ids_.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
  }
  ObArenaAllocator alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ExprFixedArray output_;
  ObDASVecIndexBruteScanCtDef ctdef_;
  ObDASVecIndexBruteScanRtDef rtdef_;
  ObDASScanCtDef scan_ctdef_;
  ObDASBaseCtDef *children_[1];
  ObExpr sort_expr_;
  ObExpr vector_expr_;
  ObExpr vid_expr_;
  transaction::ObTxDesc tx_desc_;
  transaction::ObTxReadSnapshot snapshot_;
  CountingScan input_;
  ObDASVecIndexBruteScanIterParam param_;
};

TEST_F(TestDASVectorBuildCleanup, real_partial_init_releases_context_but_preserves_input)
{
  ObDASVecIndexBruteScanIter parent;
  // The real initializer creates its memory context before checking scan layout.
  EXPECT_EQ(OB_ERR_UNEXPECTED, parent.init(param_));
  ASSERT_NE(nullptr, parent.mem_context_);
  EXPECT_FALSE(parent.is_init_complete());
  EXPECT_EQ(OB_SUCCESS, parent.release());
  EXPECT_EQ(nullptr, parent.mem_context_);
  EXPECT_EQ(0, input_.releases_);
  EXPECT_TRUE(input_.is_inited());
}

TEST_F(TestDASVectorBuildCleanup, factory_failure_does_not_consume_input)
{
  ObDASVecIndexBruteScanIter *parent = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObDASIterUtils::create_das_iter(alloc_, param_, parent));
  EXPECT_EQ(nullptr, parent);
  EXPECT_EQ(0, input_.releases_);
  EXPECT_TRUE(input_.is_inited());
}

TEST_F(TestDASVectorBuildCleanup, real_success_transfers_input_and_releases_once)
{
  prepare_valid_output();
  ObDASVecIndexBruteScanIter parent;
  ASSERT_EQ(OB_SUCCESS, parent.init(param_));
  EXPECT_TRUE(parent.is_init_complete());
  EXPECT_EQ(OB_SUCCESS, parent.release());
  EXPECT_EQ(nullptr, parent.mem_context_);
  EXPECT_EQ(1, input_.releases_);
  EXPECT_EQ(OB_SUCCESS, parent.release());
  EXPECT_EQ(1, input_.releases_);
}

TEST_F(TestDASVectorBuildCleanup, adopted_spiv_scans_are_not_borrowed_init_inputs)
{
  // push_inv_scan_iter transfers ownership independently of the init parameter.
  ObDASSPIVMergeIter parent;
  CountingScan second;
  ASSERT_EQ(OB_SUCCESS, parent.push_inv_scan_iter(&input_));
  ASSERT_EQ(OB_SUCCESS, parent.push_inv_scan_iter(&second));
  EXPECT_FALSE(parent.is_init_complete());
  EXPECT_EQ(OB_SUCCESS, parent.release());
  EXPECT_EQ(1, input_.releases_);
  EXPECT_EQ(1, second.releases_);
  EXPECT_EQ(0, parent.inv_dim_scan_iters_.count());
  EXPECT_EQ(OB_SUCCESS, parent.release());
  EXPECT_EQ(1, input_.releases_);
  EXPECT_EQ(1, second.releases_);
}
} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

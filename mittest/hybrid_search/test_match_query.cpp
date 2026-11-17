/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "test_hybrid_search.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#define private public
#define protected public
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_search_context.h"
#include "storage/retrieval/ob_inv_idx_param_cache.h"
#undef private
#undef protected

namespace oceanbase
{
// Palf-KV belongs to the external control plane and is unavailable in mittest,
// while SS bootstrap touches it through the rootserver paths below.
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
// Points to main()'s stack buffer, valid for the whole RUN_ALL_TESTS() call.
char *shared_storage_info = nullptr;
} // namespace oceanbase

class TestHybridSearchMatchQuery : public TestHybridSearch
{
public:
  TestHybridSearchMatchQuery()
    : TestHybridSearch("test_match_query")
  {
    GCONF.ob_startup_mode.set_value("shared_storage");
  }

  void SetUp() override
  {
    TestHybridSearch::SetUp();
    ASSERT_TRUE(GCTX.is_shared_storage_mode());
  }

  int create_match_node(
      ObDASIRScanCtDef *ir_ctdef,
      ObDASIRScanRtDef *ir_rtdef,
      ObExpr *query_text,
      ObExpr *boost,
      ObExpr *match_op,
      ObExpr *msm,
      ObDASMatchCtDef *&out_ctdef,
      ObDASMatchRtDef *&out_rtdef)
  {
    int ret = OB_SUCCESS;
    out_ctdef = alloc<ObDASMatchCtDef>(das_alloc_);
    out_rtdef = alloc<ObDASMatchRtDef>();
    if (OB_ISNULL(out_ctdef) || OB_ISNULL(out_rtdef)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate match ctdef or match rtdef", K(ret));
    } else {
      out_ctdef->query_text_ = query_text;
      out_ctdef->boost_ = boost;
      out_ctdef->match_operator_ = match_op;
      out_ctdef->minimum_should_match_ = msm;
      out_ctdef->ir_ctdef_idx_ = 0;

      out_ctdef->children_cnt_ = 1;
      out_ctdef->children_ = static_cast<ObDASBaseCtDef **>(das_alloc_.alloc(sizeof(ObDASBaseCtDef *)));
      out_rtdef->children_cnt_ = 1;
      out_rtdef->children_ = static_cast<ObDASBaseRtDef **>(das_alloc_.alloc(sizeof(ObDASBaseRtDef *)));

      if (OB_ISNULL(out_ctdef->children_) || OB_ISNULL(out_rtdef->children_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate children array", K(ret));
      } else {
        out_ctdef->children_[0] = ir_ctdef;
        out_rtdef->children_[0] = ir_rtdef;

        out_rtdef->ctdef_ = out_ctdef;
        out_rtdef->eval_ctx_ = eval_ctx_;
      }
    }
    return ret;
  }
};

// 收窄为与 UT 一致的能力验证：prepare_query_tokens 缓存 + 注入 bm25 cache 后的
// compute_cost。不走 Fusion / create_das_search_context / 真实 async generate_op。
TEST_F(TestHybridSearchMatchQuery, test_match_query_cost_and_tokens)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t_match", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t_match(c1 int, c2 varchar(128), key i1(c1)) organization heap", affected_rows));

  prepare_schema_info("t_match");
  prepare_test_env();

  int64_t i1_id = 0;
  ObTabletID i1_tablet_id;
  ObLSID i1_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, i1_id, "i1"));
  ASSERT_GT(i1_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, i1_id, i1_tablet_id, i1_ls_id));

  // 手工准备 mock expr frame，避免依赖真实列表达式 / Fusion 路径
  char *mock_frame = static_cast<char *>(das_alloc_.alloc(4096));
  ASSERT_NE(nullptr, mock_frame);
  MEMSET(mock_frame, 0, 4096);
  char **frames = static_cast<char **>(das_alloc_.alloc(sizeof(char *)));
  ASSERT_NE(nullptr, frames);
  frames[0] = mock_frame;
  exec_ctx_.set_frames(frames);
  exec_ctx_.set_frame_cnt(1);
  ASSERT_EQ(nullptr, eval_ctx_);
  eval_ctx_ = new ObEvalCtx(exec_ctx_);
  ASSERT_NE(nullptr, eval_ctx_);
  eval_ctx_->max_batch_size_ = max_batch_size_;

  auto setup_mock_expr = [&](ObExpr *expr, uint32_t slot) {
    expr->frame_idx_ = 0;
    expr->datum_off_ = slot * sizeof(ObDatum);
    expr->eval_info_off_ = 256 + slot * sizeof(ObEvalInfo);
    expr->res_buf_off_ = 512 + slot * 128;
    expr->res_buf_len_ = 128;
    expr->eval_func_ = nullptr;
  };

  ObExpr *query_text_expr = alloc<ObExpr>();
  ObExpr *boost_expr = alloc<ObExpr>();
  ObExpr *match_op_expr = alloc<ObExpr>();
  ObExpr *msm_expr = alloc<ObExpr>();
  setup_mock_expr(query_text_expr, 0);
  setup_mock_expr(boost_expr, 1);
  setup_mock_expr(match_op_expr, 2);
  setup_mock_expr(msm_expr, 3);

  query_text_expr->obj_meta_.set_varchar();
  query_text_expr->obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

  ObDatum &query_datum = query_text_expr->locate_datum_for_write(*eval_ctx_);
  query_datum.set_string(ObString::make_string("ocean base"));

  ObDatum &boost_datum = boost_expr->locate_datum_for_write(*eval_ctx_);
  boost_datum.set_double(1.0);

  ObDatum &op_datum = match_op_expr->locate_datum_for_write(*eval_ctx_);
  op_datum.set_int(ObMatchOperator::MATCH_OPERATOR_AND);

  ObDatum &msm_datum = msm_expr->locate_datum_for_write(*eval_ctx_);
  msm_datum.set_int(1);

  ObDASIRScanCtDef *ir_ctdef = alloc<ObDASIRScanCtDef>(das_alloc_);
  ObDASIRScanRtDef *ir_rtdef = alloc<ObDASIRScanRtDef>();

  ObDASScalarScanCtDef *inv_idx_ctdef = alloc<ObDASScalarScanCtDef>(das_alloc_);
  ObDASScalarScanRtDef *inv_idx_rtdef = alloc<ObDASScalarScanRtDef>();

  inv_idx_ctdef->ref_table_id_ = i1_id;
  inv_idx_ctdef->table_param_.set_is_partition_table(false);
  // 与 UT 一致：补齐 FTS parser，保证 prepare_query_tokens 可 tokenize
  inv_idx_ctdef->table_param_.parser_name_ = ObString::make_string("space.1");
  inv_idx_ctdef->table_param_.parser_properties_ = ObString::make_string(
      "{\"min_token_size\":1,\"max_token_size\":84,\"stopword_table\":\"none\","
      "\"dict_table\":\"none\",\"quanitfier_table\":\"none\",\"ngram_token_size\":2}");
  inv_idx_ctdef->table_param_.fts_index_type_ = share::schema::OB_FTS_INDEX_TYPE_MATCH;

  ir_ctdef->use_scalar_scan_def_ = true;
  ir_ctdef->is_async_index_ = true;
  ir_ctdef->children_cnt_ = 1;
  ir_ctdef->children_ = static_cast<ObDASBaseCtDef **>(das_alloc_.alloc(sizeof(ObDASBaseCtDef *)));
  ASSERT_NE(nullptr, ir_ctdef->children_);
  ir_ctdef->children_[0] = inv_idx_ctdef;

  ir_rtdef->ctdef_ = ir_ctdef;
  ir_rtdef->children_cnt_ = 1;
  ir_rtdef->children_ = static_cast<ObDASBaseRtDef **>(das_alloc_.alloc(sizeof(ObDASBaseRtDef *)));
  ASSERT_NE(nullptr, ir_rtdef->children_);
  ir_rtdef->children_[0] = inv_idx_rtdef;
  inv_idx_rtdef->eval_ctx_ = eval_ctx_;

  ObDASMatchCtDef *match_ctdef = nullptr;
  ObDASMatchRtDef *match_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_match_node(ir_ctdef, ir_rtdef, query_text_expr, boost_expr,
                                           match_op_expr, msm_expr, match_ctdef, match_rtdef));
  EXPECT_EQ(tenant_id_,
      match_rtdef->query_tokens_.block_allocator_.get_attr().tenant_id_);

  // 手工构造 search_ctx + related tablet，不走 Fusion / create_das_search_context
  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;

  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);

  root_scan_op.get_related_ctdefs().set_capacity(1);
  root_scan_op.get_related_rtdefs().set_capacity(1);
  root_scan_op.get_related_tablet_ids().set_capacity(1);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.get_related_ctdefs().push_back(inv_idx_ctdef));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.get_related_rtdefs().push_back(inv_idx_rtdef));
  ASSERT_EQ(OB_SUCCESS, root_scan_op.get_related_tablet_ids().push_back(i1_tablet_id));

  ObDASSearchCtx search_ctx(das_alloc_, root_scan_op);
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
  search_ctx.table_row_count_ = ObDASSearchCost(1000);

  // prepare_query_tokens 缓存
  EXPECT_FALSE(match_rtdef->query_tokens_prepared_);
  ASSERT_EQ(OB_SUCCESS, match_rtdef->prepare_query_tokens());
  EXPECT_TRUE(match_rtdef->query_tokens_prepared_);
  EXPECT_GT(match_rtdef->query_tokens_.count(), 0);
  const int64_t token_cnt = match_rtdef->query_tokens_.count();
  ASSERT_EQ(OB_SUCCESS, match_rtdef->prepare_query_tokens());
  EXPECT_EQ(token_cnt, match_rtdef->query_tokens_.count());

  // 向 bm25_param_cache_ 注入 token doc_cnt，避免真实 async 倒排估算
  ObInvIdxParamCache &cache = search_ctx.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(i1_tablet_id, ObString::make_string("ocean"), 100));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(i1_tablet_id, ObString::make_string("base"), 50));

  ObDASSearchCost cost;
  ASSERT_EQ(OB_SUCCESS, match_rtdef->compute_cost(search_ctx, cost));
  EXPECT_TRUE(cost.is_valid());
  // AND：min(100, 50) = 50
  EXPECT_EQ(50, cost.cost());
  EXPECT_TRUE(match_rtdef->query_tokens_prepared_);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WDIAG");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  char storage_info[oceanbase::share::OB_MAX_BACKUP_DEST_LENGTH] = {0};
  const int ret = oceanbase::unittest::build_hybrid_search_shared_storage_info(
      "test_match_query", storage_info, static_cast<int64_t>(sizeof(storage_info)));
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to build shared storage info, ret=%d\n", ret);
    return 1;
  }
  oceanbase::shared_storage_info = storage_info;
  return RUN_ALL_TESTS();
}

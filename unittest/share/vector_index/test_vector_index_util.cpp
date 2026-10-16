/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include "lib/oblog/ob_log.h"
#define private public
#include "share/vector_index/ob_vector_index_util.h"
#undef private
#include "sql/engine/expr/ob_expr_vector.h"

namespace oceanbase
{
namespace share
{

TEST(TestVectorIndexUtil, hnsw_build_parameter_physical_degree_boundaries)
{
  const ObVectorIndexAlgorithmType types[] = {VIAT_HNSW, VIAT_HNSW_SQ, VIAT_HNSW_BQ, VIAT_HGRAPH};
  const int64_t ef_values[] = {10, 11, 15, 19, 20, 21};
  for (const auto type : types) {
    for (const bool support_duplicate : {false, true}) {
      for (const int64_t ef : ef_values) {
        SCOPED_TRACE(::testing::Message() << "type=" << type
            << ", duplicate=" << support_duplicate << ", ef=" << ef);
        ObVectorIndexParam param;
        param.type_ = type;
        param.m_ = 10;
        param.ef_construction_ = ef;
        param.support_duplicate_ = support_duplicate;
        const bool valid = VIAT_HNSW == type && !support_duplicate ? ef > 10 : ef >= 20;
        EXPECT_EQ(valid ? OB_SUCCESS : OB_NOT_SUPPORTED,
                  ObVectorIndexUtil::validate_hnsw_build_params(param));
      }
    }
  }
}

TEST(TestVectorIndexUtil, alter_validates_physical_degree)
{
  const char *invalid_params[] = {
      "TYPE=HNSW,M=10,EF_CONSTRUCTION=15,SUPPORT_DUPLICATE=TRUE",
      "TYPE=HNSW_SQ,M=10,EF_CONSTRUCTION=15,SUPPORT_DUPLICATE=FALSE",
      "TYPE=HNSW_BQ,M=10,EF_CONSTRUCTION=15,SUPPORT_DUPLICATE=FALSE"};
  for (const char *params : invalid_params) {
    SCOPED_TRACE(params);
    EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::validate_alter_hnsw_index_params(
        common::ObString::make_string(params), false));
  }
  EXPECT_EQ(OB_SUCCESS, ObVectorIndexUtil::validate_alter_hnsw_index_params(
      common::ObString::make_string("TYPE=HNSW,M=10,EF_CONSTRUCTION=15,SUPPORT_DUPLICATE=FALSE"),
      false));
  EXPECT_EQ(OB_SUCCESS, ObVectorIndexUtil::validate_alter_hnsw_index_params(
      common::ObString::make_string("TYPE=HNSW,M=10,EF_CONSTRUCTION=20,SUPPORT_DUPLICATE=TRUE"),
      false));
}

TEST(TestVectorIndexUtil, extra_info_hnsw_validates_physical_degree)
{
  ObVectorIndexParam param;
  param.type_ = VIAT_HNSW;
  param.m_ = 10;
  param.extra_info_actual_size_ = 8;
  param.support_duplicate_ = false;
  for (const int64_t ef : {19, 20, 21}) {
    SCOPED_TRACE(ef);
    param.ef_construction_ = ef;
    EXPECT_EQ(ef >= 20 ? OB_SUCCESS : OB_NOT_SUPPORTED,
              ObVectorIndexUtil::validate_hnsw_build_params(param));
  }
  EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::validate_alter_hnsw_index_params(
      common::ObString::make_string(
          "TYPE=HNSW,M=10,EF_CONSTRUCTION=19,EXTRA_INFO_ACTUAL_SIZE=8"), false));
  EXPECT_EQ(OB_SUCCESS, ObVectorIndexUtil::validate_alter_hnsw_index_params(
      common::ObString::make_string(
          "TYPE=HNSW,M=10,EF_CONSTRUCTION=20,EXTRA_INFO_ACTUAL_SIZE=8"), false));
}

TEST(TestVectorIndexUtil, rebuild_validates_inherited_build_parameters)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const common::ObString old_params = common::ObString::make_string(
      "DISTANCE=L2,TYPE=HNSW,LIB=VSAG,M=10,EF_CONSTRUCTION=15,EF_SEARCH=64");
  for (const char *patch : {"SUPPORT_DUPLICATE=TRUE", "TYPE=HNSW_SQ", "TYPE=HNSW_BQ"}) {
    SCOPED_TRACE(patch);
    common::ObString new_params = common::ObString::make_string(patch);
    EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::construct_rebuild_index_param(
        table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator));
  }
  common::ObString new_params = common::ObString::make_string(
      "SUPPORT_DUPLICATE=TRUE,EF_CONSTRUCTION=20");
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::construct_rebuild_index_param(
      table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator));
  ObVectorIndexParam param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      new_params, VIT_HNSW_INDEX, param, false));
  EXPECT_EQ(10, param.m_);
  EXPECT_EQ(20, param.ef_construction_);
  EXPECT_TRUE(param.support_duplicate_);
}

TEST(TestVectorIndexUtil, rebuild_validates_inherited_duplicate_mode)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const common::ObString old_params = common::ObString::make_string(
      "DISTANCE=L2,TYPE=HNSW,LIB=VSAG,M=10,EF_CONSTRUCTION=20,EF_SEARCH=64,SUPPORT_DUPLICATE=TRUE");
  common::ObString new_params = common::ObString::make_string("EF_CONSTRUCTION=15");
  EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::construct_rebuild_index_param(
      table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator));
  new_params = common::ObString::make_string("SUPPORT_DUPLICATE=FALSE,EF_CONSTRUCTION=15");
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::construct_rebuild_index_param(
      table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator));
  ObVectorIndexParam param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      new_params, VIT_HNSW_INDEX, param, false));
  EXPECT_FALSE(param.support_duplicate_);
  EXPECT_EQ(15, param.ef_construction_);
}

TEST(TestVectorIndexUtil, l1_parameter_and_expression_mapping)
{
  ObVectorIndexParam param;
  const common::ObString params =
      common::ObString::make_string("DISTANCE=L1, TYPE=HNSW, LIB=VSAG");
  ASSERT_EQ(OB_SUCCESS,
      ObVectorIndexUtil::parser_params_from_string(
          params, ObVectorIndexType::VIT_HNSW_INDEX, param, false));
  EXPECT_EQ(ObVectorIndexDistAlgorithm::VIDA_L1, param.dist_algorithm_);
  EXPECT_EQ(ObVectorIndexAlgorithmType::VIAT_HNSW, param.type_);
  EXPECT_STREQ("l1",
      VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_L1]);
  EXPECT_STREQ("l1",
      ObVectorIndexUtil::get_dist_algorithm_str(
          ObVectorIndexDistAlgorithm::VIDA_L1));

  EXPECT_TRUE(ObVectorIndexUtil::is_expr_type_and_distance_algorithm_match(
      T_FUN_SYS_L1_DISTANCE, ObVectorIndexDistAlgorithm::VIDA_L1));
  EXPECT_FALSE(ObVectorIndexUtil::is_expr_type_and_distance_algorithm_match(
      T_FUN_SYS_L2_DISTANCE, ObVectorIndexDistAlgorithm::VIDA_L1));

  int64_t dis_type = sql::ObExprVectorDistance::ObVecDisType::MAX_TYPE;
  ASSERT_EQ(OB_SUCCESS,
      ObVectorIndexUtil::get_vec_dis_type_from_dis_algorithm(
          ObVectorIndexDistAlgorithm::VIDA_L1, dis_type));
  EXPECT_EQ(sql::ObExprVectorDistance::ObVecDisType::MANHATTAN, dis_type);
}

TEST(TestVectorIndexUtil, rebuild_rejects_l1_quantized_hnsw)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const common::ObString old_params = common::ObString::make_string(
      "DISTANCE=L1,TYPE=HNSW,LIB=VSAG,M=16,EF_CONSTRUCTION=200,EF_SEARCH=64");
  const char *unsupported_params[] = {
      "DISTANCE=L1,TYPE=HNSW_SQ", "DISTANCE=L1,TYPE=HNSW_BQ"};
  for (const char *params : unsupported_params) {
    SCOPED_TRACE(params);
    common::ObString new_params = common::ObString::make_string(params);
    EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::construct_rebuild_index_param(
        table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator, 128));
  }
}

TEST(TestVectorIndexUtil, rebuild_preserves_supported_metric_type_combinations)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const char *old_params[] = {
      "DISTANCE=L1,TYPE=HNSW,LIB=VSAG,M=16,EF_CONSTRUCTION=200,EF_SEARCH=64",
      "DISTANCE=L2,TYPE=HNSW,LIB=VSAG,M=16,EF_CONSTRUCTION=200,EF_SEARCH=64",
      "DISTANCE=L1,TYPE=IVF_FLAT,LIB=OB,NLIST=16,SAMPLE_PER_NLIST=256"};
  const char *requested_params[] = {
      "DISTANCE=L1,TYPE=HNSW", "DISTANCE=L2,TYPE=HNSW_SQ", "DISTANCE=L1,TYPE=IVF_FLAT"};
  const ObVectorIndexType index_types[] = {VIT_HNSW_INDEX, VIT_HNSW_INDEX, VIT_IVF_INDEX};
  const ObVectorIndexAlgorithmType expected_types[] = {VIAT_HNSW, VIAT_HNSW_SQ, VIAT_IVF_FLAT};
  const ObVectorIndexDistAlgorithm expected_metrics[] = {VIDA_L1, VIDA_L2, VIDA_L1};
  for (int64_t i = 0; i < 3; ++i) {
    SCOPED_TRACE(requested_params[i]);
    common::ObString new_params = common::ObString::make_string(requested_params[i]);
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::construct_rebuild_index_param(
        table_schema, common::ObString::make_string(old_params[i]), new_params,
        index_types[i], &allocator, 128));
    ObVectorIndexParam parsed;
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
        new_params, index_types[i], parsed, false));
    EXPECT_EQ(expected_types[i], parsed.type_);
    EXPECT_EQ(expected_metrics[i], parsed.dist_algorithm_);
  }
}

TEST(TestVectorIndexUtil, rebuild_rejects_l1_spfresh)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const common::ObString old_params = common::ObString::make_string(
      "DISTANCE=L1,TYPE=IVF_FLAT,LIB=OB,NLIST=16,SAMPLE_PER_NLIST=256");
  common::ObString new_params = common::ObString::make_string(
      "DISTANCE=L1,TYPE=IVF_FLAT,CENTROIDS_FRESH_MODE=SPFRESH");
  EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::construct_rebuild_index_param(
      table_schema, old_params, new_params, VIT_IVF_INDEX, &allocator, 128));
}

TEST(TestVectorIndexUtil, existing_metric_values_are_stable)
{
  EXPECT_EQ(0, static_cast<int>(ObVectorIndexDistAlgorithm::VIDA_L2));
  EXPECT_EQ(1, static_cast<int>(ObVectorIndexDistAlgorithm::VIDA_IP));
  EXPECT_EQ(2, static_cast<int>(ObVectorIndexDistAlgorithm::VIDA_COS));
  EXPECT_STREQ("l2", VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_L2]);
  EXPECT_STREQ("ip", VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_IP]);
  EXPECT_STREQ("cosine", VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_COS]);
}

TEST(TestVectorIndexUtil, unknown_metric_is_still_rejected)
{
  ObVectorIndexParam param;
  const common::ObString params =
      common::ObString::make_string("DISTANCE=UNKNOWN, TYPE=HNSW");
  EXPECT_EQ(OB_NOT_SUPPORTED,
      ObVectorIndexUtil::parser_params_from_string(
          params, ObVectorIndexType::VIT_HNSW_INDEX, param, false));
}

TEST(TestVectorIndexUtil, l1_vsag_distance_converts_to_ob_distance)
{
  constexpr double vsag_distance = 12.5;
  double ob_distance = 0;
  ASSERT_EQ(OB_SUCCESS,
      ObVectorIndexUtil::convert_vsag_to_ob_distance(
          vsag_distance, T_FUN_SYS_L1_DISTANCE, ob_distance));
  EXPECT_DOUBLE_EQ(vsag_distance, ob_distance);
}

TEST(TestVectorIndexUtil, l1_ob_distance_converts_to_vsag_distance)
{
  constexpr double ob_distance = 12.5;
  double vsag_distance = 0;
  ASSERT_EQ(OB_SUCCESS,
      ObVectorIndexUtil::convert_ob_to_vsag_distance(
          ob_distance, T_FUN_SYS_L1_DISTANCE, vsag_distance));
  EXPECT_DOUBLE_EQ(ob_distance, vsag_distance);
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

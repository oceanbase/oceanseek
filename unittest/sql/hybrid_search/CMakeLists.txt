sql_unittest(test_das_fusion_iter)
sql_unittest(test_hybrid_fusion_op)
target_sources(test_hybrid_fusion_op PRIVATE
  ../engine/basic/utils/expr_maker.cpp
  ../engine/basic/utils/mock_child.cpp
  ../engine/basic/utils/test_op_base.cpp)

sql_unittest(test_hybrid_search_dsl_parameterization)
sql_unittest(test_async_index_plan_constraint)

file(COPY remove_pointer.py DESTINATION .)

# FIXME: disable for now
#sql_unittest(test_optimizer)
sql_unittest(test_json_format)
sql_unittest(test_explain_json_format)
sql_unittest(test_async_ivf_index_selection)
sql_unittest(test_opt_expr_min_max)
# sql_unittest(test_opt_est_sel)
sql_unittest(test_skyline_prunning)
# sql_unittest(test_route_policy)
# sql_unittest(test_location_part_id)
# FIXME: disable for now
#sql_unittest(test_join_order)
#sql_unittest(cost_model_util)

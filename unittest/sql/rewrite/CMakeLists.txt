function(rewrite_unittest case)
  sql_unittest(${ARGV})
  target_sources(${case} PRIVATE)
endfunction()

rewrite_unittest(test_query_range)
rewrite_unittest(test_raw_expr_equal)
# Fix me: disable for now
#rewrite_unittest(test_transformer)
rewrite_unittest(test_rls_rewrite_mysql)
rewrite_unittest(test_transform_exempt_rls_mysql)
rewrite_unittest(test_rowid_generated_column)

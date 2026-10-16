function(schema_unittest case)
  ob_unittest(${ARGV})
  target_sources(${case}
    PRIVATE
    mock_schema_service.h
    schema_test_utils.h
    mock_multi_version_schema_service_for_fallback.h)
  target_link_libraries(${case} PRIVATE mockcontainer)
endfunction()

schema_unittest(test_table_dml_param)
schema_unittest(test_rls_schema)
schema_unittest(test_rls_resolver_helper)
schema_unittest(test_simple_schema_kv_cache)
schema_unittest(test_schema_history_sort)
